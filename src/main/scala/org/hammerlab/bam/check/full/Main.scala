package org.hammerlab.bam.check.full

import java.lang.{ Long ⇒ JLong }

import caseapp.{ ExtraName ⇒ O }
import cats.implicits.catsStdShowForLong
import cats.syntax.all._
import org.apache.spark.rdd.RDD
import org.hammerlab.app.{ SparkPathApp, SparkPathAppArgs }
import org.hammerlab.bam.check.PosMetadata.showRecord
import org.hammerlab.bam.check.full.error.Flags.{ TooFewFixedBlockBytes, toCounts }
import org.hammerlab.bam.check.full.error.{ Counts, Flags }
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.check.{ AnalyzeCalls, Blocks, PosMetadata }
import org.hammerlab.bam.header.Header
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ PosIterator, SeekableUncompressedBytes }
import org.hammerlab.bytes.Bytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.guava.collect.RangeSet
import org.hammerlab.io.Printer._
import org.hammerlab.io.SampleSize
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.magic.rdd.SampleRDD._
import org.hammerlab.math.Monoid.zero
import org.hammerlab.math.MonoidSyntax._
import org.hammerlab.paths.Path

import scala.collection.immutable.SortedMap
import org.hammerlab.bam.check.ParseRanges.parser

case class Args(@O("g") bgzfBlockHeadersToCheck: Int = 5,
                @O("i") ranges: Option[RangeSet[JLong]] = None,
                @O("k") blocks: Option[Path] = None,
                @O("l") printLimit: SampleSize = SampleSize(None),
                @O("m") splitSize: Option[Bytes] = None,
                @O("o") out: Option[Path] = None,
                @O("q") resultsPerPartition: Int = 1000000,
                @O("r") records: Option[Path] = None,
                warn: Boolean = false
               )
  extends SparkPathAppArgs
    with Blocks.Args
    with IndexedRecordPositions.Args

object Main
  extends SparkPathApp[Args](classOf[Registrar])
    with AnalyzeCalls {

  override def run(args: Args): Unit = {

    val header = Header(path)
    implicit val headerBroadcast = sc.broadcast(header)
    implicit val contigLengthsBroadcast = sc.broadcast(header.contigLengths)
    implicit val rangesBroadcast = sc.broadcast(args.ranges)

    val calls =
      if (args.recordsPath.exists) {

        val (compressedSizeAccumulator, calls) =
          vsIndexed[Option[Flags], Checker](args)

        analyzeCalls(
          calls
            .map {
              case (pos, (expected, flags)) ⇒
                pos → (expected, flags.isEmpty)
            },
          args.resultsPerPartition,
          compressedSizeAccumulator
        )
        echo("")

        calls
          .map {
            case (pos, (expected, flags))
              if flags.isEmpty == expected ⇒
              pos → flags
          }
      } else {
        val blocks = Blocks(args)

        blocks
          .mapPartitions {
            blocks ⇒

              val ch = SeekableByteChannel(path).cache
              val uncompressedBytes = SeekableUncompressedBytes(ch)
              val checker = Checker(uncompressedBytes, contigLengthsBroadcast.value)

              blocks
                .flatMap(PosIterator(_))
                .map {
                  pos ⇒
                    pos →
                        checker(pos)
                }
                .finish(uncompressedBytes.close())
          }
      }

    val flagsByCount: RDD[(Int, (Pos, Flags))] =
      calls
        .flatMap {
          case (pos, Some(flags))
            if flags != TooFewFixedBlockBytes ⇒
            Some(pos → flags)
          case _ ⇒
            None
        }
        .keyBy(_._2.numNonZeroFields)

    /**
     * How many times each flag correctly rules out a [[Pos]], grouped by how many total flags ruled out that [[Pos]].
     *
     * Useful for identifying e.g. flags that tend to be "critical" (necessary to avoid [[PosResult]] read-boundary
     * [[Call]]s).
     */
    val negativesByNumNonzeroFields: Array[(Int, Counts)] =
      flagsByCount
        .map {
          case (numFlags, (_, flags)) ⇒
            numFlags → toCounts(flags)
        }
        .reduceByKey(_ |+| _, Flags.size)
        .collect()
        .sortBy(_._1)

    /**
     * CDF to [[negativesByNumNonzeroFields]]'s PDF: how many times does each flag correctly rule out [[Pos]]s that
     * were ruled out by *at most `n`* total flags, for each `n`.
     */
    val countsByNonZeroFields: SortedMap[Int, (Counts, Counts)] =
      SortedMap(
        negativesByNumNonzeroFields
          .scanLeft(
            0 → (zero[Counts], zero[Counts])
          ) {
            case (
              (_, (_, countSoFar)),
              (numNonZeroFields, count)
            ) ⇒
              numNonZeroFields →
                (
                  count,
                  countSoFar |+| count
                )
          }
          .drop(1): _*  // Discard the dummy/initial "0" entry added above to conform to [[scanLeft]] API
      )

    lazy val positionsByFlagCounts =
      SortedMap(
        flagsByCount
          .mapValues(_ ⇒ 1L)
          .reduceByKey(_ + _)
          .collect: _*
      )

    val closeCalls =
      flagsByCount
        .filter(_._1 <= 2)
        .mapPartitions {
          it ⇒
            val ch = SeekableByteChannel(path).cache
            val uncompressedBytes = SeekableUncompressedBytes(ch)
            val contigLengths = contigLengthsBroadcast.value
            it
              .map {
                case (numFlags, (pos, flags)) ⇒
                  numFlags →
                    PosMetadata(
                      uncompressedBytes,
                      pos,
                      flags,
                      headerBroadcast.value,
                      contigLengths
                    )
              }
              .finish(uncompressedBytes.close())
        }
        .cache

    /**
     * "Critical" error counts: how many times each flag was the *only* flag identifying a read-boundary-candidate as
     * false.
     */
    countsByNonZeroFields.get(1) match {
      case Some((criticalCounts, _)) ⇒
        val numCriticalCalls = positionsByFlagCounts(1)

        echo(
          "Critical error counts (true negatives where only one check failed):",
          criticalCounts.show(includeZeros = false),
          ""
        )

        val criticalCalls =
          closeCalls
            .filter(_._1 == 1)
            .values
            .sample(numCriticalCalls)

        print(
          criticalCalls,
          numCriticalCalls,
          s"$numCriticalCalls critical positions:",
          n ⇒ s"$n of $numCriticalCalls critical positions:"
        )

      case None ⇒
        echo(
          "No positions where only one check failed"
        )
    }

    echo("")

    countsByNonZeroFields.get(2) match {
      case Some((counts, _)) ⇒
        val numCloseCalls = positionsByFlagCounts(2)

        val calls =
          closeCalls
            .filter(_._1 == 2)
            .values

        val closeCallHist =
          calls
            .map(_.flags → 1L)
            .reduceByKey(_ + _)
            .map(_.swap)
            .collect
            .sortBy(-_._1)

        val sampledCalls = calls.sample(numCloseCalls)

        print(
          sampledCalls,
          numCloseCalls,
          s"$numCloseCalls positions where exactly two checks failed:",
          n ⇒ s"$n of $numCloseCalls positions where exactly two checks failed:",
          indent = "\t"
        )
        echo("")

        if (closeCallHist.head._1 > 1) {
          print(
            closeCallHist.map { case (num, flags) ⇒ show"$num:\t$flags" },
            "\tHistogram:",
            _ ⇒ "\tHistogram:",
            indent = "\t\t"
          )
          echo("")
        }

        echo(
          "\tPer-flag totals:",
          s"${
            counts.show(
              indent = "\t",
              includeZeros = false
            )
          }",
          ""
        )
      case None ⇒
        echo(
          "No positions where exactly two checks failed",
          ""
        )
    }

    /**
     * "Total" error counts: how many times each flag ruled out a position, over the entire dataset
     */
    val totalErrorCounts = countsByNonZeroFields.last._2._2

    echo(
      "Total error counts:",
      totalErrorCounts.show(hideTooFewFixedBlockBytes = true),
      ""
    )
  }
}
