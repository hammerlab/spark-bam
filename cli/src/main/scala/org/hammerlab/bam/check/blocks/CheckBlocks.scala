package org.hammerlab.bam.check.blocks

import cats.Show
import cats.Show.show
import cats.implicits.{ catsStdShowForInt, catsStdShowForLong }
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check.Checker.MakeChecker
import org.hammerlab.bam.check.eager.CheckBam
import org.hammerlab.bam.check.indexed.BlocksAndIndexedRecords
import org.hammerlab.bam.check.{ Blocks, CheckerApp, ReadStartFinder, eager, indexed, seqdoop }
import org.hammerlab.bam.kryo.pathSerializer
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.Metadata
import org.hammerlab.bytes.Bytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.cli.app.Cmd
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.kryo._
import org.hammerlab.magic.rdd.SampleRDD._
import org.hammerlab.magic.rdd.sliding.SlidingRDD._
import org.hammerlab.magic.rdd.zip.ZipPartitionsRDD._
import org.hammerlab.paths.Path
import org.hammerlab.spark.accumulator.Histogram
import org.hammerlab.stats.Stats

import scala.collection.mutable

object CheckBlocks
  extends Cmd.With[CheckBam.Opts] {

  val main = Main(makeApp)
  def makeApp(args: Args) = new CheckerApp(args, Registrar) {

    val totalCompressedSize = path.size

    val numBlocksAccumulator = sc.longAccumulator("numBlocks")//driver { sc.longAccumulator("numBlocks") }

    val blockFirstOffsetsAccumulator = Histogram[Option[Int]]("blockFirstOffsets")

    def callPartition[C1 <: ReadStartFinder, C2 <: ReadStartFinder](blocks: Iterator[(Option[Metadata], Metadata)])(
        implicit
        makeChecker1: MakeChecker[Boolean, C1],
        makeChecker2: MakeChecker[Boolean, C2]
    ): Iterator[((Long, (Option[Pos], Option[Pos])), Long)] = {
      val ch = SeekableByteChannel(path).cache
      val checker1 = makeChecker1(ch)
      val checker2 = makeChecker2(ch)

      blocks
        .flatMap {
          case (
            prevBlockOpt,
            Metadata(start, _, _)
          ) ⇒

            numBlocksAccumulator.add(1)

            val pos1 = checker1.nextReadStart(Pos(start, 0))
            val pos2 = checker2.nextReadStart(Pos(start, 0))

            val offset = pos1.filter(_.blockPos == start).map(_.offset)
            blockFirstOffsetsAccumulator.add(offset)

            if (pos1 != pos2)
              Some(
                (
                  start,
                  (pos1, pos2)
                ) →
                  prevBlockOpt
                    .map(_.compressedSize)
                    .getOrElse(1)
                    .toLong
              )
            else
              None
        }
        .finish(ch.close())
    }

    def callIndexedRDD[C <: ReadStartFinder](args: BlocksAndIndexedRecords)(
        implicit makeChecker: MakeChecker[Boolean, C]
    ): RDD[((Long, (Option[Pos], Option[Pos])), Long)] = {
      val BlocksAndIndexedRecords(blocks, records) = args
      blocks
        .sliding2Prev
        .zippartitions(records) {
          case (blocks, setsIter) ⇒
            implicit val records = setsIter.next()
            callPartition[indexed.Checker, C](blocks)
        }
      }

    override def run(): Unit = {
      val badBlocks =
        (args.sparkBam, args.hadoopBam) match {
          case (true, false) ⇒
            callIndexedRDD[eager.Checker](BlocksAndIndexedRecords())
          case (false, true) ⇒
            callIndexedRDD[seqdoop.Checker](BlocksAndIndexedRecords())
          case _ ⇒
            Blocks()
              .setName("blocks")
              .cache
              .sliding2Prev
              .mapPartitions { callPartition[eager.Checker, seqdoop.Checker] }
        }

      badBlocks
        .setName("bad-blocks")
        .cache

      import cats.implicits.{ catsKernelStdGroupForLong, catsKernelStdMonoidForTuple2 }
      import cats.syntax.all._

      val (numWrongCompressedPositions, numWrongBlocks) =
        badBlocks
          .values
          .map(_ → 1L)
          .fold(0L → 0L)(_ |+| _)

      val numBlocks = numBlocksAccumulator.value
      val blocksFirstOffsets = blockFirstOffsetsAccumulator.value

      import org.hammerlab.io.Printer._

      /**
       * Print special messages if all BGZF blocks' first read-starts are at the start of the block (additionally
       * distinguishing the case where some blocks don't contain any read-starts).
       *
       * Otherwise, print a histogram of the blocks' first-read-start offsets.
       */
      def printBlockFirstReadOffsetsInfo(): Unit =
        blocksFirstOffsets
          .keySet
          .toVector match {
            case Vector(None, Some(0)) ⇒
              echo(
                "",
                s"${blocksFirstOffsets(Some(0))} blocks start with a read, ${blocksFirstOffsets(None)} blocks didn't contain a read"
              )
            case Vector(Some(0)) ⇒
              echo(
                "",
                "All blocks start with reads"
              )
            case _ ⇒
              val nonEmptyOffsets =
                blocksFirstOffsets.collect {
                  case (Some(k), v) ⇒ k → v
                }

              implicit val truncatedDouble: Show[Double] = show { math.round(_).show }

              echo(
                "",
                s"Offsets of blocks' first reads (${blocksFirstOffsets.getOrElse(None, 0)} blocks didn't contain a read start):",
                Stats.fromHist(nonEmptyOffsets)
              )
          }

      if (numWrongBlocks == 0) {
        echo(
          s"First read-position matched in $numBlocks BGZF blocks totaling ${Bytes.format(totalCompressedSize, includeB = true)} (compressed)"
        )
        printBlockFirstReadOffsetsInfo()
      } else {
        echo(
          s"First read-position mismatched in $numWrongBlocks of $numBlocks BGZF blocks",
          "",
          s"$numWrongCompressedPositions of $totalCompressedSize (${numWrongCompressedPositions * 1.0 / totalCompressedSize}) compressed positions would lead to bad splits"
        )

        printBlockFirstReadOffsetsInfo()
        echo("")

        implicit val showPosOpt: Show[Option[Pos]] =
          show {
            _.map(_.toString).getOrElse("-")
          }

        print(
          badBlocks
            .map {
              case (
                (
                  start,
                  (pos1, pos2)
                ),
                compressedSize
              ) ⇒
                show"$start (prev block size: $compressedSize):\t$pos1\t$pos2"
            }
            .sample(numWrongBlocks),
          numWrongBlocks,
          s"$numWrongBlocks mismatched blocks:",
          (n: Int) ⇒ s"$n of $numWrongBlocks mismatched blocks:"
        )
      }
    }
  }

  case class Registrar() extends spark.Registrar(
    Blocks,
    CheckerApp,
    cls[Path],    // broadcast
    cls[mutable.WrappedArray.ofRef[_]],  // sliding RDD collect
    cls[mutable.WrappedArray.ofInt]      // sliding RDD collect
  )
}
