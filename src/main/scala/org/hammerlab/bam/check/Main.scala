package org.hammerlab.bam.check

import java.lang.{ Long ⇒ JLong }

import caseapp.core.ArgParser
import caseapp.core.ArgParser.instance
import caseapp.{ Parser, ExtraName ⇒ O }
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
import org.hammerlab.guava.collect.Range.closedOpen
import org.hammerlab.guava.collect.{ RangeSet, TreeRangeSet }
import org.hammerlab.io.SampleSize
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.paths.Path
import ParseRanges.parser

object ParseRanges {

  val rangeRegex = """(.*)-(.*)""".r
  val chunkRegex = """(.+)\+(.+)""".r
  val pointRegex = """(.+)""".r

  implicit val parser: ArgParser[RangeSet[JLong]] =
    instance[RangeSet[JLong]]("ranges") {
      str ⇒
        val rangeSet = TreeRangeSet.create[JLong]()
        str
          .split(",")
          .map {
            case rangeRegex(from, until) ⇒
              closedOpen[JLong](
                if (from.isEmpty)
                  0L
                else
                  Bytes(from).bytes,
                Bytes(until).bytes
              )
            case chunkRegex(fromStr, length) ⇒
              val from = Bytes(fromStr).bytes
              closedOpen[JLong](
                from,
                from + Bytes(length).bytes
              )
            case pointRegex(pointStr) ⇒
              val point = pointStr.toLong
              closedOpen[JLong](
                point,
                point + 1
              )
          }
          .foreach(rangeSet.add)

        Right(rangeSet)
    }
}

/**
 * CLI for [[Main]]: check every (bgzf-decompressed) byte-position in a BAM file for a record-start with and compare the results to the true
 * read-start positions.
 *
 * - Takes one argument: the path to a BAM file.
 * - Requires that BAM to have been indexed prior to running by [[org.hammerlab.bgzf.index.IndexBlocks]] and
 *   [[org.hammerlab.bam.index.IndexRecords]].
 *
 * @param blocks file with bgzf-block-start positions as output by [[org.hammerlab.bgzf.index.IndexBlocks]]
 * @param records file with BAM-record-start positions as output by [[org.hammerlab.bam.index.IndexRecords]]
 * @param eager if set, run a [[org.hammerlab.bam.check.eager.Checker]], which marks a position as "negative" and
 *              returns as soon as any check fails.
 */
case class Args(@O("e") eager: Boolean = false,
                @O("f") full: Boolean = false,
                @O("g") bgzfBlockHeadersToCheck: Int = 5,
                @O("i") @transient ranges: Option[RangeSet[JLong]] = None,
                @O("k") blocks: Option[Path] = None,
                @O("l") printLimit: SampleSize = SampleSize(None),
                @O("m") splitSize: Option[Bytes] = None,
                @O("o") out: Option[Path] = None,
                @O("q") resultsPerPartition: Int = 1000000,
                @O("r") records: Option[Path] = None,
                @O("s") seqdoop: Boolean = false,
                warn: Boolean = false)
  extends SparkPathAppArgs
    with Blocks.Args
    with IndexedRecordPositions.Args

object Main
  extends SparkPathApp[Args](classOf[Registrar])
    with AnalyzeCalls {

//  val parser = implicitly[Parser[Args]]

  override def run(args: Args): Unit = {

    if (args.warn)
      getRootLogger.setLevel(WARN)

    val header = Header(path)
    implicit val headerBroadcast = sc.broadcast(header)
    implicit val contigLengthsBroadcast = sc.broadcast(header.contigLengths)
    implicit val rangesBroadcast = sc.broadcast(args.ranges)

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

    val blocks = Blocks(args)

    val compressedSizeAccumulator = sc.longAccumulator("compressedSizeAccumulator")

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
