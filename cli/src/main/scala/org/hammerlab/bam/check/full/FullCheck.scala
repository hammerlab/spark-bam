package org.hammerlab.bam.check.full

import caseapp.{ AppName, ProgName, Recurse }
import hammerlab.iterator._
import hammerlab.monoid._
import hammerlab.path._
import hammerlab.print._
import magic_rdds.sample._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.args.{ FindReadArgs, LogArgs, PostPartitionArgs }
import org.hammerlab.bam.check.PosMetadata.showRecord
import org.hammerlab.bam.check.full.error.Flags.TooFewFixedBlockBytes
import org.hammerlab.bam.check.full.error.{ Counts, Flags, Result }
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.check.{ Blocks, CallPartition, CheckerApp, MaxReadSize, PosMetadata, ReadsToCheck }
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bam.spark.Split
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ Metadata, PosIterator, SeekableUncompressedBytes }
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.cli.app.Cmd
import org.hammerlab.cli.args.PrintLimitArgs
import org.hammerlab.kryo._

import scala.collection.immutable.SortedMap
import scala.collection.mutable

object FullCheck extends Cmd {

  @AppName("Check all uncompressed positions in a BAM with the 'full' checker; print statistics about which checks fail how often")
  @ProgName("… org.hammerlab.bam.check.full.Main")
  case class Opts(@Recurse blocks: Blocks.Args,
                  @Recurse records: IndexedRecordPositions.Args,
                  @Recurse logging: LogArgs,
                  @Recurse printLimit: PrintLimitArgs,
                  @Recurse partitioning: PostPartitionArgs,
                  @Recurse findReadArgs: FindReadArgs
                 )

  def closeCallsWithMetadata(it: Iterator[(Int, (Pos, Flags))])(implicit 
                                                                path: Path,
                                                                header: Broadcast[Header],
                                                                readsToCheck: ReadsToCheck,
                                                                maxReadSize: MaxReadSize
  ) = {
    val ch = SeekableByteChannel(path).cache

    implicit val uncompressedBytes = SeekableUncompressedBytes(ch)

    it
      .map {
        case (numFlags, (pos, flags)) ⇒
          numFlags →
            PosMetadata(
              pos,
              flags
            )
      }
      .finish(uncompressedBytes.close())
  }

  def checkPartition(blocks: Iterator[Metadata])(implicit
                                                 path: Path,
                                                 contigLengthsBroadcast: Broadcast[ContigLengths],
                                                 readsToCheck: ReadsToCheck) = {
    val ch = SeekableByteChannel(path).cache
    val uncompressedBytes = SeekableUncompressedBytes(ch)
    val checker =
      Checker(
        uncompressedBytes,
        contigLengthsBroadcast.value,
        readsToCheck
      )

    blocks
    .flatMap(PosIterator(_))
    .map {
      pos ⇒
        pos →
          checker(pos)
    }
    .finish(uncompressedBytes.close())
  }
  
  val main = Main(makeApp)
  def makeApp(args: Args) =
    new CheckerApp(args, Registrar)
      with CallPartition {

      val calls =
        if (args.records.path.exists) {

          val calls = vsIndexed[Result, Checker]

          apply(
            calls
              .map {
                case (pos, (expected, result)) ⇒
                  pos → ((expected, result.call))
              },
            args.partitioning.resultsPerPartition
          )
          echo("")

          calls
            .map {
              case (pos, (expected, result)) ⇒
                if (result.call == expected)
                  pos → result
                else
                  throw new IllegalStateException(
                    s"False ${if (result.call) "positive" else "negative"} at $pos: $result"
                  )
            }
        } else
          Blocks()
            .mapPartitions {
              blocks ⇒

                val ch = SeekableByteChannel(path).cache
                val uncompressedBytes = SeekableUncompressedBytes(ch)
                val checker =
                  Checker(
                    uncompressedBytes,
                    contigLengthsBroadcast.value,
                    readsToCheck
                  )

                blocks
                  .flatMap(PosIterator(_))
                  .map {
                    pos ⇒
                      pos →
                          checker(pos)
                  }
                  .finish(uncompressedBytes.close())
            }

      val flagsByCount: RDD[(Int, (Pos, Flags))] =
        calls
          .flatMap {
            case (pos, flags: Flags)
              if flags != TooFewFixedBlockBytes ⇒
              Some(pos → flags)
            case _ ⇒
              None
          }
          .keyBy(_._2.numNonZeroFields)
          .setName("flagsByCount")
          .cache

      /**
       * How many times each flag correctly rules out a [[Pos]], grouped by how many total flags rule out that [[Pos]].
       *
       * Useful for identifying e.g. flags that tend to be "critical" (necessary to avoid false-positive read-boundary
       * calls).
       */
      val negativesByNumNonzeroFields: Array[(Int, Counts)] =
        flagsByCount
          .mapValues {
            case (_, flags) ⇒
              flags.toCounts
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

              implicit val uncompressedBytes = SeekableUncompressedBytes(ch)

              it
                .map {
                  case (numFlags, (pos, flags)) ⇒
                    numFlags →
                      PosMetadata(
                        pos,
                        flags
                      )
                }
                .finish(uncompressedBytes.close())
          }
          .setName("closeCalls")
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
              criticalCounts.lines(includeZeros = false),
              ""
            )

            val criticalCalls =
              closeCalls
                .filter(_._1 == 1)
                .values
                .sample(numCriticalCalls)

            implicit val contigLengths = contigLengthsBroadcast.value

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

            val closePositions =
              closeCalls
                .filter(_._1 == 2)
                .values
                .setName("closePositions")
                .cache

            val closeCallHist =
              closePositions
                .map(_.flags → 1L)
                .reduceByKey(_ + _)
                .map(_.swap)
                .collect
                .sortBy(-_._1)

            print(
              closePositions.sample(numCloseCalls),
              numCloseCalls,
              s"$numCloseCalls positions where exactly two checks failed:",
              n ⇒ s"$n of $numCloseCalls positions where exactly two checks failed:"
            )
            echo("")

            if (closeCallHist.head._1 > 1) {
              indent {
                print(
                  closeCallHist.map { case (num, flags) ⇒ show"$num:\t$flags" },
                  "Histogram:",
                  _ ⇒ "Histogram:"
                )
              }
              echo("")
            }

            indent {
              echo(
                "Per-flag totals:",
                counts.lines(includeZeros = false)
              )
            }
            echo("")
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
        totalErrorCounts.lines(hideTooFewFixedBlockBytes = true),
        ""
      )
    }

  case class Registrar() extends spark.Registrar(
    CheckerApp,
    CallPartition,
    Blocks,
    cls[Flags],
    cls[Counts],
    arr[PosMetadata],
    cls[mutable.WrappedArray.ofRef[_]],
    arr[Split],
    cls[mutable.WrappedArray.ofInt]
  )
}
