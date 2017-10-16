package org.hammerlab.bam.check.blocks

import cats.Show
import cats.implicits.{ catsStdShowForInt, catsStdShowForLong }
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.hammerlab.bam.check.Checker.MakeChecker
import org.hammerlab.bam.check.eager.CheckBam
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.check.{ Blocks, CheckerApp, MaxReadSize, ReadStartFinder, eager, indexed, seqdoop }
import org.hammerlab.bam.kryo.pathSerializer
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.Metadata
import org.hammerlab.bytes.Bytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.cli.app
import org.hammerlab.cli.app.Args
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.kryo._
import org.hammerlab.magic.rdd.SampleRDD._
import org.hammerlab.magic.rdd.sliding.SlidingRDD._
import org.hammerlab.magic.rdd.zip.ZipPartitionsRDD._
import org.hammerlab.paths.Path
import org.hammerlab.spark.accumulator.Histogram
import org.hammerlab.stats.Stats

import scala.collection.immutable.SortedSet
import scala.collection.mutable

object CheckBlocks {

  type Opts = CheckBam.Opts

  case class PartitionConfig(numBlocksAccumulator: LongAccumulator,
                             blockFirstOffsetsAccumulator: Histogram[Option[Int]],
                             maxReadSize: MaxReadSize)
  
  def callPartition(blocks: Iterator[(Option[Metadata], Metadata)])(
      implicit
      config: PartitionConfig,
      checkers: (ReadStartFinder, ReadStartFinder)
  ): Iterator[((Long, (Option[Pos], Option[Pos])), Long)] = {
    val (checker1, checker2) = checkers
    implicit val PartitionConfig(
      numBlocksAccumulator,
      blockFirstOffsetsAccumulator,
      maxReadSize
    ) = config
    
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
  
  def callRDD[C1 <: ReadStartFinder, C2 <: ReadStartFinder](rdd: RDD[(Option[Metadata], Metadata)])(
      implicit
      path: Path,
      config: PartitionConfig,
      makeChecker1: MakeChecker[Boolean, C1],
      makeChecker2: MakeChecker[Boolean, C2]
  ): RDD[((Long, (Option[Pos], Option[Pos])), Long)] =
    rdd.mapPartitions {
      blocks ⇒
        implicit val ch = SeekableByteChannel(path).cache
        implicit val checkers = (makeChecker1(ch), makeChecker2(ch))
        callPartition(blocks).finish(ch.close())
    }

  def callIndexedRDD[C <: ReadStartFinder](args: (RDD[Metadata], RDD[SortedSet[Pos]]))(
      implicit
      path: Path,
      config: PartitionConfig,
      makeChecker: MakeChecker[Boolean, C]
  ): RDD[((Long, (Option[Pos], Option[Pos])), Long)] = {
    val (rdd, records) = args
    rdd
      .sliding2Prev
      .zippartitions(records) {
        case (blocks, setsIter) ⇒
          implicit val records = setsIter.next()
          implicit val ch = SeekableByteChannel(path).cache
          implicit val checkers = (indexed.Checker.makeChecker.apply(ch), makeChecker(ch))
          callPartition(blocks).finish(ch.close())
      }
  }
  
  case class App(args: Args[Opts])
    extends CheckerApp(args, Registrar) {

    val totalCompressedSize = path.size

    val numBlocksAccumulator = sc.longAccumulator("numBlocks")
    val blockFirstOffsetsAccumulator = Histogram[Option[Int]]("blockFirstOffsets")

    implicit val config = PartitionConfig(numBlocksAccumulator, blockFirstOffsetsAccumulator, maxReadSize)

    val badBlocks =
      (args.sparkBam, args.hadoopBam) match {
        case (true, false) ⇒
          callIndexedRDD[eager.Checker](IndexedRecordPositions())
        case (false, true) ⇒
          callIndexedRDD[seqdoop.Checker](IndexedRecordPositions())
        case _ ⇒
          callRDD[eager.Checker, seqdoop.Checker](
            Blocks()
              .setName("blocks")
              .cache
              .sliding2Prev
          )
      }
    
//    badBlocks.take(10)

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
        s"First read-position mismatched in $numWrongBlocks of $numBlocks BGZF blocks",
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

  case class Registrar() extends spark.Registrar(
    Blocks,
    CheckerApp,
    cls[Path],    // broadcast
    cls[mutable.WrappedArray.ofRef[_]],  // sliding RDD collect
    cls[mutable.WrappedArray.ofInt]      // sliding RDD collect
  )

  object Main extends app.Main(App)
}
