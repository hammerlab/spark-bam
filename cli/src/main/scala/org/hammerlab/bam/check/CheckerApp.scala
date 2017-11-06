package org.hammerlab.bam.check

import hammerlab.iterator._
import magic_rdds.sample._
import magic_rdds.size._
import org.apache.log4j.Level.WARN
import org.apache.log4j.Logger.getRootLogger
import org.apache.spark.rdd.RDD
import org.hammerlab.args.{ ByteRanges, FindReadArgs, LogArgs }
import org.hammerlab.bam.check.full.error.{ Flags, Success }
import org.hammerlab.bam.check.indexed.{ BlocksAndIndexedRecords, IndexedRecordPositions }
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.bytes.Bytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.cli.app.Args
import org.hammerlab.cli.app.HasPrintLimit.PrintLimit
import org.hammerlab.cli.app.OutPathApp.HasOverwrite
import org.hammerlab.cli.app.close.Closeable
import org.hammerlab.cli.app.spark.{ PathApp, Registrar }
import org.hammerlab.kryo._
import org.hammerlab.shapeless._
import org.hammerlab.shapeless.hlist.Find

/**
 * Container for extracting some common types from arguments and a target BAM file and making them implicitly available
 * in subclasses.
 */
abstract class CheckerApp[Opts: HasOverwrite : PrintLimit](args: Args[Opts],
                                                           reg: Registrar)(
    implicit
    c: Closeable,
    findBlocks: Find[Opts, Blocks.Args],
    findIndexedRecordPositions: Find[Opts, IndexedRecordPositions.Args],
    findLog: Find[Opts, LogArgs],
    findFindReads: Find[Opts, FindReadArgs]
)
  extends PathApp(args, reg)
    with hammerlab.show
    with Serializable {

  implicit val blocksArgs = args.findt[Blocks.Args]
  implicit val recordPosArgs = args.findt[IndexedRecordPositions.Args]
  implicit val logging = args.findt[LogArgs]
  implicit val findReadArgs = args.findt[FindReadArgs]

  @transient lazy val header = Header(path)
  implicit val headerBroadcast = sc.broadcast { header }
  implicit val contigLengthsBroadcast = sc.broadcast { header.contigLengths }
  implicit val contigLengths = contigLengthsBroadcast.value

  implicit val rangesBroadcast = sc.broadcast { blocksArgs.ranges }

  implicit val readsToCheck: ReadsToCheck = findReadArgs.readsToCheck
  implicit val maxReadSize: MaxReadSize = findReadArgs.maxReadSize

  val compressedSizeAccumulator = sc.longAccumulator("compressedSize")

  if (logging.warn)
    getRootLogger.setLevel(WARN)

  def apply(calls: RDD[(Pos, (Boolean, Boolean))],
            resultsPerPartition: Int): Unit = {

    val  truePositiveAccumulator = sc.longAccumulator( "truePositives")
    val  trueNegativeAccumulator = sc.longAccumulator( "trueNegatives")
    val falsePositiveAccumulator = sc.longAccumulator("falsePositives")
    val falseNegativeAccumulator = sc.longAccumulator("falseNegatives")

    val differingCalls =
      calls
        .filter {
          case (_, (expected, actual)) ⇒
            (expected, actual) match {
              case ( true,  true) ⇒
                truePositiveAccumulator.add(1)
                false
              case ( true, false) ⇒
                falseNegativeAccumulator.add(1)
                true
              case (false,  true) ⇒
                falsePositiveAccumulator.add(1)
                true
              case (false, false) ⇒
                trueNegativeAccumulator.add(1)
                false
            }
        }
        .mapValues { case (actual, _) ⇒ actual }
        .setName("originalDifferingCalls")
        .cache

    val fps = differingCalls.filter(!_._2).keys
    val fns = differingCalls.filter( _._2).keys

//    val pathBroadcast = sc.broadcast(path)

    implicit val contigLengthsBroadcast = sc.broadcast(header.contigLengths)

    val fpsWithMetadata =
      fps
        .mapPartitions {
          it ⇒
            val ch = SeekableByteChannel(path).cache

            implicit val uncompressedBytes = SeekableUncompressedBytes(ch)

            val fullChecker =
              full.Checker(
                uncompressedBytes,
                contigLengthsBroadcast.value,
                readsToCheck
              )

            it
            .map {
              pos ⇒
                PosMetadata(
                  pos,
                  fullChecker(pos) match {
                    case Success(n) ⇒
                      throw new IllegalThreadStateException(
                        s"Full checker false-positive at $pos: $n reads parsed successfully"
                      )
                    case flags: Flags ⇒ flags
                  }
                )
            }
            .finish(uncompressedBytes.close())
        }
        .setName("fpsWithMetadata")
        .cache

    // Force materialization so that accumulator values are computed
    fpsWithMetadata.size

    // Blocks are materialized for the first time by the above job, so we can now read this data
    val compressedSize = compressedSizeAccumulator.value

    val  numTruePositives =  truePositiveAccumulator.value.toLong
    val  numTrueNegatives =  trueNegativeAccumulator.value.toLong
    val numFalsePositives = falsePositiveAccumulator.value.toLong
    val numFalseNegatives = falseNegativeAccumulator.value.toLong

    val numReads = numTruePositives + numFalseNegatives

    val totalCalls =
      numReads +
        numTrueNegatives +
        numFalsePositives

    val compressionRatio = totalCalls.toDouble / compressedSize

    echo(
      s"$totalCalls uncompressed positions",
      s"${Bytes.format(compressedSize)} compressed",
      "Compression ratio: %.2f".format(compressionRatio),
      s"$numReads reads"
    )

    def printFalsePositives(): Unit = {
      val flagsHist =
        fpsWithMetadata
          .map(_.flags → 1L)
          .reduceByKey(_ + _)
          .collect()
          .sortBy(-_._2)
      print(
        flagsHist
          .map {
            case (flags, count) ⇒
              show"$count:\t$flags"
          },
        "False-positive-site flags histogram:",
        _ ⇒ "False-positive-site flags histogram:"
      )
      echo("")

      implicit val contigLengthsBroadcast = sc.broadcast(header.contigLengths)
      implicit val contigLengths = contigLengthsBroadcast.value

      import PosMetadata.showRecord

      val sampledPositions =
        fpsWithMetadata
          // Optimization: convert to strings before collecting, otherwise reads can be huge due to denormalized headers
          .map {
            fp ⇒
              fp.show
          }
          .sample(numFalsePositives)

      print(
        sampledPositions,
        numFalsePositives,
        s"False positives with succeeding read info:",
        n ⇒ s"$n of $numFalsePositives false positives with succeeding read info::"
      )
    }

    def printFalseNegatives(): Unit =
      print(
        fns.sample(numFalseNegatives),
        numFalseNegatives,
        s"$numFalseNegatives false negatives:",
        n ⇒ s"$n of $numFalseNegatives false negatives:"
      )

    (numFalsePositives > 0, numFalseNegatives > 0) match {
      case (false, false) ⇒
        echo("All calls matched!")
      case (falsePositives, falseNegatives) ⇒
        echo(
          s"$numFalsePositives false positives, $numFalseNegatives false negatives",
          ""
        )

        if (falsePositives)
          printFalsePositives()

        if (falseNegatives)
          printFalseNegatives()
    }
  }
}

object CheckerApp extends spark.Registrar(
  cls[Header],
  cls[ContigLengths],
  cls[Option[_]],
  cls[ByteRanges],
  BlocksAndIndexedRecords
)
