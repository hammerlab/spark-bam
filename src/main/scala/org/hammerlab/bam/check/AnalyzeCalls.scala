package org.hammerlab.bam.check

import cats.implicits._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.hammerlab.app.SparkPathApp
import org.hammerlab.args.ByteRanges
import org.hammerlab.bam.check.Checker.{ MakeChecker, MaxReadSize, ReadsToCheck }
import org.hammerlab.bam.check.full.error.{ Flags, Result, Success }
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ PosIterator, SeekableUncompressedBytes }
import org.hammerlab.bytes.Bytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.io.Printer.{ echo, print }
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.magic.rdd.SampleRDD._
import org.hammerlab.magic.rdd.partitions.OrderedRepartitionRDD._
import org.hammerlab.magic.rdd.size._
import org.hammerlab.magic.rdd.zip.ZipPartitionsRDD._
import org.hammerlab.math.ceil
import org.hammerlab.paths.Path

trait AnalyzeCalls {
  self: SparkPathApp[_] ⇒

  def analyzeCalls(calls: RDD[(Pos, (Boolean, Boolean))],
                   resultsPerPartition: Int,
                   compressedSizeAccumulator: LongAccumulator)(
      implicit
      headerBroadcast: Broadcast[Header],
      contigLengthsBroadcast: Broadcast[ContigLengths],
      readsToCheck: ReadsToCheck,
      maxReadSize: MaxReadSize
  ): Unit = {

    val  truePositiveAccumulator = sc.longAccumulator( "truePositives")
    val  trueNegativeAccumulator = sc.longAccumulator( "trueNegatives")
    val falsePositiveAccumulator = sc.longAccumulator("falsePositives")
    val falseNegativeAccumulator = sc.longAccumulator("falseNegatives")

    val differingCalls = {
      val originalDifferingCalls =
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
          .map { case (pos, (actual, _)) ⇒ pos → actual }
          .setName("originalDifferingCalls")
          .cache

      val numDifferingCalls = originalDifferingCalls.size

      originalDifferingCalls
        .orderedRepartition(
          ceil(
            numDifferingCalls,
            resultsPerPartition
          )
          .toInt
        )
        .setName("repartitioned-differing-calls")
        .cache
    }

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

    val fps = differingCalls.filter(!_._2).keys
    val fns = differingCalls.filter( _._2).keys

    val pathBroadcast = sc.broadcast(path)

    val fpsWithMetadata =
      fps
        .mapPartitions {
          it ⇒
            val ch = SeekableByteChannel(pathBroadcast.value).cache

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

      import PosMetadata.showRecord

      val sampledPositions =
        fpsWithMetadata
          // Optimization: convert to strings before collecting, otherwise reads can be huge due to denormalized headers
          .map(_.show)
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

  def vsIndexed[Call, C <: Checker[Call]](
      implicit
      path: Path,
      sc: SparkContext,
      makeChecker: MakeChecker[Call, C],
      rangesBroadcast: Broadcast[Option[ByteRanges]],
      blockArgs: Blocks.Args,
      recordArgs: IndexedRecordPositions.Args
  ): (LongAccumulator, RDD[(Pos, (Boolean, Call))]) = {

    val (blocks, bounds) = Blocks()

    val posBounds =
      bounds
        .copy(
          partitions =
            bounds
            .partitions
            .map {
              _.map {
                case (start, endOpt) ⇒
                  Pos(start, 0) → endOpt.map(Pos(_, 0))
              }
            }
        )

    val indexedRecords = IndexedRecordPositions(recordArgs.path)

    val repartitionedRecords = indexedRecords.toSets(posBounds)

    val compressedSizeAccumulator = sc.longAccumulator("compressedSize")

    val calls =
      blocks
        .zippartitions(repartitionedRecords) {
          (blocks, setsIter) ⇒
            val recordsSet = setsIter.next()

            val ch = SeekableByteChannel(path).cache
            val checker = makeChecker(ch)

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
                      recordsSet(pos),
                      checker(pos)
                    )
              }
              .finish(ch.close())
        }

    (compressedSizeAccumulator, calls)
  }
}
