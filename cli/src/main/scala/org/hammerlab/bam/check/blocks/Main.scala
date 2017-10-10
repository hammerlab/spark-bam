package org.hammerlab.bam.check.blocks

import cats.Show
import cats.implicits.{ catsStdShowForInt, catsStdShowForLong }
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{ AccumulatorV2, LongAccumulator }
import org.hammerlab.args.ByteRanges
import org.hammerlab.bam.check.Checker.MakeChecker
import org.hammerlab.bam.check.eager.Args
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.check.{ Blocks, CheckerMain, MaxReadSize, ReadStartFinder, eager, indexed, seqdoop }
import org.hammerlab.bam.kryo.pathSerializer
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.Metadata
import org.hammerlab.bytes.Bytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.cli.app.SparkPathApp
import org.hammerlab.kryo._
import org.hammerlab.magic.rdd.SampleRDD._
import org.hammerlab.magic.rdd.sliding.SlidingRDD._
import org.hammerlab.magic.rdd.zip.ZipPartitionsRDD._
import org.hammerlab.paths.Path
import org.hammerlab.stats.Stats

import scala.collection.immutable.SortedMap
import scala.collection.mutable

case class HistAccumulator[T: Ordering](var map: mutable.Map[T, Long] = mutable.Map.empty[T, Long])
  extends AccumulatorV2[T, SortedMap[T, Long]] {
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[T, SortedMap[T, Long]] =
    HistAccumulator(map.clone())

  override def reset(): Unit = map = mutable.Map.empty[T, Long]

  override def add(k: T): Unit =
    map.update(
      k,
      map.getOrElse(k, 0L) + 1
    )

  override def merge(other: AccumulatorV2[T, SortedMap[T, Long]]): Unit =
    for {
      (k, v) ← other.value
    } {
      map.update(k, map.getOrElse(k, 0L) + v)
    }

  override def value: SortedMap[T, Long] = SortedMap(map.toSeq: _*)
}

object HistAccumulator {
  def apply[T: Ordering](name: String)(implicit sc: SparkContext): HistAccumulator[T] = {
    val a = HistAccumulator[T]()
    sc.register(a, name)
    a
  }
}

class Registrar extends spark.Registrar(
  Blocks,
  CheckerMain,
  cls[Path],    // broadcast
  cls[mutable.WrappedArray.ofRef[_]],  // sliding RDD collect
  cls[mutable.WrappedArray.ofInt]      // sliding RDD collect
)

object Main
  extends SparkPathApp[Args, Registrar] {

  def callPartition[C1 <: ReadStartFinder, C2 <: ReadStartFinder](blocks: Iterator[(Option[Metadata], Metadata)])(
      implicit
      pathBroadcast: Broadcast[Path],
      numBlocksAccumulator: LongAccumulator,
      blockFirstOffsetsAccumulator: HistAccumulator[Option[Int]],
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

  def compare[C1 <: ReadStartFinder, C2 <: ReadStartFinder](
      implicit
      pathBroadcast: Broadcast[Path],
      args: Blocks.Args,
      numBlocksAccumulator: LongAccumulator,
      blockFirstOffsetsAccumulator: HistAccumulator[Option[Int]],
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

  def vsIndexed[C <: ReadStartFinder](
      implicit
      pathBroadcast: Broadcast[Path],
      sc: SparkContext,
      makeChecker: MakeChecker[Boolean, C],
      rangesBroadcast: Broadcast[Option[ByteRanges]],
      numBlocksAccumulator: LongAccumulator,
      blockFirstOffsetsAccumulator: HistAccumulator[Option[Int]],
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

  override protected def run(args: Args): Unit = {
    new CheckerMain(args) {
      override def run(): Unit = {

        implicit val totalCompressedSize = path.size
        implicit val numBlocksAccumulator = sc.longAccumulator("numBlocks")
        implicit val blockFirstOffsetsAccumulator = HistAccumulator[Option[Int]]("blockFirstOffsets")
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

          print[String](
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
  }
}
