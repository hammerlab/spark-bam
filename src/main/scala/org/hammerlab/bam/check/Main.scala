package org.hammerlab.bam.check

import caseapp.{ ExtraName ⇒ O }
import org.apache.log4j.Level.WARN
import org.apache.log4j.Logger.getRootLogger
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.hammerlab.app.{ SparkPathApp, SparkPathAppArgs }
import org.hammerlab.bam.check.Checker.MakeChecker
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.header.Header
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.PosIterator
import org.hammerlab.bytes.Bytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.io.SampleSize
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.paths.Path

/**
 * CLI for [[Main]]: check every (bgzf-decompressed) byte-position in a BAM file with a [[Checker]] and compare the
 * results to the true read-start positions.
 *
 * - Takes one argument: the path to a BAM file.
 * - Requires that BAM to have been indexed prior to running by [[org.hammerlab.bgzf.index.IndexBlocks]] and
 *   [[org.hammerlab.bam.index.IndexRecords]].
 *
 * @param blocks file with bgzf-block-start positions as output by [[org.hammerlab.bgzf.index.IndexBlocks]]
 * @param records file with BAM-record-start positions as output by [[org.hammerlab.bam.index.IndexRecords]]
 * @param numBlocks if set, only check the first [[numBlocks]] bgzf blocks of
 * @param blocksWhitelist if set, only process the bgzf blocks at these positions (comma-seperated)
 * @param blocksPerPartition process this many blocks in each partition
 * @param eager if set, run a [[org.hammerlab.bam.check.eager.Checker]], which marks a position as "negative" and
 *              returns as soon as any check fails.
 */
case class Args(@O("e") eager: Boolean = false,
                @O("f") full: Boolean = false,
                @O("g") bgzfBlockHeadersToCheck: Int = 5,
                @O("i") blocksPerPartition: Int = 20,
                @O("k") blocks: Option[Path] = None,
                @O("l") printLimit: SampleSize = SampleSize(None),
                @O("m") splitSize: Option[Bytes] = None,
                @O("n") numBlocks: Option[Int] = None,
                @O("o") out: Option[Path] = None,
                @O("q") resultsPerPartition: Int = 1000000,
                @O("r") records: Option[Path] = None,
                @O("s") seqdoop: Boolean = false,
                @O("w") blocksWhitelist: Option[String] = None,
                warn: Boolean = false)
  extends SparkPathAppArgs
    with Blocks.Args
    with IndexedRecordPositions.Args

object Main
  extends SparkPathApp[Args](classOf[Registrar])
    with AnalyzeCalls {

  override def run(args: Args): Unit = {

    if (args.warn)
      getRootLogger.setLevel(WARN)

    val header = Header(path)
    implicit val headerBroadcast = sc.broadcast(header)
    implicit val contigLengthsBroadcast = sc.broadcast(header.contigLengths)

    val (compressedSizeAccumulator, calls) =
      (args.eager, args.seqdoop) match {
        case (true, false) ⇒
          vsIndexed[Boolean, eager.Checker](args)
        case (false, true) ⇒
          vsIndexed[Boolean, seqdoop.Checker](args)
        case _ ⇒
          compare[
            eager.Checker,
            seqdoop.Checker
          ](
            args
          )
      }

    analyzeCalls(
      calls,
      args.resultsPerPartition,
      compressedSizeAccumulator
    )
  }

  def compare[C1 <: Checker[Boolean], C2 <: Checker[Boolean]](args: Args)(
      implicit
      path: Path,
      makeChecker1: MakeChecker[Boolean, C1],
      makeChecker2: MakeChecker[Boolean, C2]
  ): (LongAccumulator, RDD[(Pos, (Boolean, Boolean))]) = {

    val (blocks, _) = Blocks(args)

    val compressedSizeAccumulator = sc.longAccumulator("compressedSize")

    val calls =
      blocks
        .mapPartitions {
          blocks ⇒
            val ch = SeekableByteChannel(path).cache
            val checker1 = makeChecker1(ch)
            val checker2 = makeChecker2(ch)

            blocks
              .flatMap {
                block ⇒
                  compressedSizeAccumulator.add(block.compressedSize)
                  PosIterator(block)
              }
              .map {
                pos ⇒
                  pos →
                    (
                      checker1(pos),
                      checker2(pos)
                    )
              }
              .finish(ch.close())
        }

    (compressedSizeAccumulator, calls)
  }
}
