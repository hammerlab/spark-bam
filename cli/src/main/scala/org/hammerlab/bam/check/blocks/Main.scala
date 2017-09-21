package org.hammerlab.bam.check.blocks

import cats.Show
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.hammerlab.args.ByteRanges
import org.hammerlab.bam.check.Checker.MakeChecker
import org.hammerlab.bam.check.eager.Args
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.check.{ Blocks, CheckerMain, MaxReadSize, ReadStartFinder, eager, indexed, seqdoop }
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.Metadata
import org.hammerlab.bytes.Bytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.cli.app.SparkPathApp
import org.hammerlab.magic.rdd.SampleRDD._
import org.hammerlab.magic.rdd.sliding.SlidingRDD._
import org.hammerlab.magic.rdd.zip.ZipPartitionsRDD._
import org.hammerlab.paths.Path

object Main
  extends SparkPathApp[Args](classOf[Registrar]) {

  def callPartition[C1 <: ReadStartFinder, C2 <: ReadStartFinder](blocks: Iterator[(Option[Metadata], Metadata)])(
      implicit
      pathBroadcast: Broadcast[Path],
      numBlocksAccumulator: LongAccumulator,
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

        import cats.implicits._

        val (numWrongCompressedPositions, numWrongBlocks) =
          badBlocks
            .values
            .map(_ → 1L)
            .fold(0L → 0L)(_ |+| _)

        val numBlocks = numBlocksAccumulator.value

        import org.hammerlab.io.Printer._

        if (numWrongBlocks == 0)
          echo(
            s"First read-position matched in $numBlocks BGZF blocks totaling ${Bytes.format(totalCompressedSize, includeB = true)} (compressed)"
          )
        else {
          echo(
            s"First read-position mis-matchined in $numWrongBlocks of $numBlocks BGZF blocks",
            "",
            s"$numWrongCompressedPositions of $totalCompressedSize (${numWrongCompressedPositions * 1.0 / totalCompressedSize}) compressed positions would lead to bad splits",
            ""
          )

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
