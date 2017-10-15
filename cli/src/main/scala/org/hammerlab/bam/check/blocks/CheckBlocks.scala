package org.hammerlab.bam.check.blocks

import cats.Show
import cats.implicits.{ catsStdShowForInt, catsStdShowForLong }
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.hammerlab.args.ByteRanges
import org.hammerlab.bam.check.Checker.MakeChecker
import org.hammerlab.bam.check.eager.CheckBam
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.check.{ Blocks, CheckerMain, MaxReadSize, ReadStartFinder, eager, indexed, seqdoop }
import org.hammerlab.bam.kryo.pathSerializer
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.Metadata
import org.hammerlab.bytes.Bytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.cli.app
import org.hammerlab.cli.app.Args
import org.hammerlab.cli.app.spark.PathApp
import org.hammerlab.kryo._
import org.hammerlab.magic.rdd.SampleRDD._
import org.hammerlab.magic.rdd.sliding.SlidingRDD._
import org.hammerlab.magic.rdd.zip.ZipPartitionsRDD._
import org.hammerlab.paths.Path
import org.hammerlab.spark.accumulator.Histogram
import org.hammerlab.stats.Stats

import scala.collection.mutable

object CheckBlocks {

  type Opts = CheckBam.Opts

  def callPartition[C1 <: ReadStartFinder, C2 <: ReadStartFinder](blocks: Iterator[(Option[Metadata], Metadata)])(
      implicit
      pathBroadcast: Broadcast[Path],
      numBlocksAccumulator: LongAccumulator,
      blockFirstOffsetsAccumulator: Histogram[Option[Int]],
      totalCompressedSize: Long,
      makeChecker1: MakeChecker[Boolean, C1],
      makeChecker2: MakeChecker[Boolean, C2],
      maxReadSize: MaxReadSize
  ): Iterator[((Long, (Option[Pos], Option[Pos])), Long)] = {

    val ch = SeekableByteChannel(pathBroadcast.value).cache

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
  }

  case class App(args: Args[Opts])
    extends PathApp(args, Registrar) {

    def vsIndexed[C <: ReadStartFinder](
        implicit
        pathBroadcast: Broadcast[Path],
        sc: SparkContext,
        makeChecker: MakeChecker[Boolean, C],
        rangesBroadcast: Broadcast[Option[ByteRanges]],
        numBlocksAccumulator: LongAccumulator,
        blockFirstOffsetsAccumulator: Histogram[Option[Int]],
        totalCompressedSize: Long,
        blockArgs: Blocks.Args,
        recordArgs: IndexedRecordPositions.Args,
        maxReadSize: MaxReadSize
    ): RDD[((Long, (Option[Pos], Option[Pos])), Long)] = {
      val (blocks, records) = IndexedRecordPositions()
      blocks
      .sliding2Prev
      .zippartitions(records) {
        case (blocks, setsIter) ⇒
          implicit val records = setsIter.next()
          callPartition[indexed.Checker, C](blocks)
      }
    }

    def compare[C1 <: ReadStartFinder, C2 <: ReadStartFinder](
        implicit
        pathBroadcast: Broadcast[Path],
        args: Blocks.Args,
        numBlocksAccumulator: LongAccumulator,
        blockFirstOffsetsAccumulator: Histogram[Option[Int]],
        makeChecker1: MakeChecker[Boolean, C1],
        makeChecker2: MakeChecker[Boolean, C2],
        maxReadSize: MaxReadSize
    ): RDD[((Long, (Option[Pos], Option[Pos])), Long)] = {
      implicit val totalCompressedSize = path.size
      val Blocks(blocks, _) = Blocks()
      blocks
        .setName("blocks")
        .cache
        .sliding2Prev
        .mapPartitions {
          blocks ⇒
            callPartition[C1, C2](blocks)
        }
    }

    new CheckerMain(opts) {

      implicit val totalCompressedSize = path.size
      implicit val numBlocksAccumulator = sc.longAccumulator("numBlocks")
      implicit val blockFirstOffsetsAccumulator = Histogram[Option[Int]]("blockFirstOffsets")
      implicit val pathBroadcast = sc.broadcast(path)

      val badBlocks =
        (args.sparkBam, args.hadoopBam) match {
          case (true, false) ⇒
            vsIndexed[eager.Checker]
          case (false, true) ⇒
            vsIndexed[seqdoop.Checker]
          case _ ⇒
            compare[
              eager.Checker,
              seqdoop.Checker
            ]
        }

      badBlocks
        .setName("bad-blocks")
        .cache

      import cats.implicits.{ catsKernelStdGroupForLong, catsKernelStdMonoidForTuple2 }
      import cats.syntax.all._

      val ord = implicitly[Ordering[(Long, (Option[Pos], Option[Pos]))]]

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

              implicit val truncatedDouble: Show[Double] =
                Show.show { math.round(_).show }

              import Stats.showRational

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
          s"First read-position mis-matched in $numWrongBlocks of $numBlocks BGZF blocks",
          "",
          s"$numWrongCompressedPositions of $totalCompressedSize (${numWrongCompressedPositions * 1.0 / totalCompressedSize}) compressed positions would lead to bad splits"
        )

        printBlockFirstReadOffsetsInfo()
        echo("")

        import cats.Show.show

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
    CheckerMain,
    cls[Path],    // broadcast
    cls[mutable.WrappedArray.ofRef[_]],  // sliding RDD collect
    cls[mutable.WrappedArray.ofInt]      // sliding RDD collect
  )

  object Main extends app.Main(App)
}
