package org.hammerlab.bam.check

import cats.Show
import cats.Show.show
import cats.implicits.catsStdShowForInt
import cats.syntax.all._
import htsjdk.samtools.SAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.hammerlab.app.SparkPathApp
import org.hammerlab.bam.check
import org.hammerlab.bam.check.Checker.MakeChecker
import org.hammerlab.bam.check.full.Main.path
import org.hammerlab.bam.check.full.error.Flags
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bam.iterator.RecordStream
import org.hammerlab.bam.spark.FindRecordStart
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ PosIterator, SeekableUncompressedBytes }
import org.hammerlab.bytes.Bytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.io.Printer.{ echo, print }
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.magic.rdd.SampleRDD.sample
import org.hammerlab.magic.rdd.partitions.OrderedRepartitionRDD._
import org.hammerlab.magic.rdd.partitions.SortedRDD.bounds
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
      contigLengthsBroadcast: Broadcast[ContigLengths]
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

    val  numTruePositives =  truePositiveAccumulator.value
    val  numTrueNegatives =  trueNegativeAccumulator.value
    val numFalsePositives = falsePositiveAccumulator.value
    val numFalseNegatives = falseNegativeAccumulator.value

    val numDifferingCalls = numFalseNegatives + numFalsePositives

    val numReads = numTruePositives + numFalseNegatives

    val totalCalls =
      numReads +
        numTrueNegatives +
        numFalsePositives

    val compressionRatio = totalCalls.toDouble / compressedSize

    val fps = differingCalls.filter(!_._2).keys
    val fns = differingCalls.filter(_._2).keys

    val fpsWithMetadata =
      fps
        .mapPartitions {
          it ⇒
            val ch = SeekableByteChannel(path).cache
            val uncompressedBytes = SeekableUncompressedBytes(ch)
            val contigLengths = contigLengthsBroadcast.value
            val fullChecker = full.Checker(uncompressedBytes, contigLengths)
            it
              .map {
                pos ⇒
                  PosMetadata(
                    uncompressedBytes,
                    pos,
                    fullChecker(pos)
                      .getOrElse(
                        throw new IllegalThreadStateException(
                          s"Full checker false-positive at $pos"
                        )
                      ),
                    headerBroadcast.value,
                    contigLengths
                  )
              }
              .finish(uncompressedBytes.close())
        }

    echo(
      s"$totalCalls uncompressed positions",
      s"${Bytes.format(compressedSize)} compressed",
      "Compression ratio: %.2f".format(compressionRatio),
      s"$numReads reads"
    )

    def printFalsePositives(): Unit = {
      val sampledPositions =
        sample(
          fpsWithMetadata,
          numFalsePositives
        )

      import PosMetadata.showRecord
      import PosMetadata.defaultShow

      print(
        sampledPositions,
        numDifferingCalls,
        s"$numFalsePositives false positives:",
        n ⇒ s"$n of $numFalsePositives false positives:"
      )
    }

    def printFalseNegatives(): Unit =
      print(
        sample(
          fns,
          numFalseNegatives
        ),
        numFalseNegatives,
        s"$numFalseNegatives false negatives:",
        n ⇒ s"$n of $numFalseNegatives false negatives:"
      )

    (numFalsePositives > 0, numFalseNegatives > 0) match {
      case (false, false) ⇒
        echo("All calls matched!")
      case (falsePositives, falseNegatives) ⇒
        echo(s"$numFalsePositives false positives, $numFalseNegatives false negatives")
        if (falsePositives)
          printFalsePositives()

        if (falseNegatives)
          printFalseNegatives()
    }
  }

  def vsIndexed[Call, C <: Checker[Call]](args: Blocks.Args with IndexedRecordPositions.Args)(
      implicit
      path: Path,
      sc: SparkContext,
      makeChecker: MakeChecker[Call, C]
  ): (LongAccumulator, RDD[(Pos, (Boolean, Call))]) = {

    val (blocks, whitelist) = Blocks(args)

    val whitelistBroadcast = sc.broadcast(whitelist)

    val indexedRecords =
      IndexedRecordPositions(
        args.recordsPath,
        whitelistBroadcast
      )
      .toSets(
        bounds(
          blocks
            .map(
              block ⇒
                Pos(
                  block.start,
                  0
                )
            )
        )
      )

    val compressedSizeAccumulator = sc.longAccumulator("compressedSize")

    val calls =
      blocks
        .zippartitions(indexedRecords) {
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

case class NextRecord(record: SAMRecord, delta: Int)

object NextRecord {
  implicit def makeShow(implicit showRecord: Show[SAMRecord]): Show[NextRecord] =
    show {
      case NextRecord(record, delta) ⇒
        show"$delta before $record"
    }
}

case class PosMetadata(pos: Pos,
                       recordOpt: Option[NextRecord],
                       flags: Flags)

object PosMetadata {

  implicit def defaultShow(implicit showRecord: Show[SAMRecord]): Show[PosMetadata] =
    show {
      case PosMetadata(pos, recordOpt, flags) ⇒
        show"$pos:\t$recordOpt. Failing checks: $flags"
    }

  implicit def showNextRecordOpt(implicit showNextRecord: Show[NextRecord]): Show[Option[NextRecord]] =
    show {
      case Some(nextRecord) ⇒ nextRecord.show
      case None ⇒ "no next record"
    }

  def recordPos(record: SAMRecord)(implicit contigLengthsBroadcast: Broadcast[ContigLengths]): String =
    s"${contigLengthsBroadcast.value.apply(record.getReferenceIndex)._1}:${record.getStart}"

  implicit def showRecord(implicit contigLengthsBroadcast: Broadcast[ContigLengths]): Show[SAMRecord] =
    show {
      record ⇒
        record
          .toString
          .dropRight(1) +  // remove trailing period
          (
            if (record.getReadUnmappedFlag && record.getStart >= 0)
              s" (placed at ${recordPos(record)})"
            else if (!record.getReadUnmappedFlag)
              s" @ ${recordPos(record)}"
            else
              ""
          )
    }

  def apply(uncompressedBytes: SeekableUncompressedBytes,
            pos: Pos,
            flags: Flags,
            header: Header,
            contigLengths: ContigLengths): PosMetadata =
    PosMetadata(
      pos,
      {
        FindRecordStart
        .withDelta(
          uncompressedBytes,
          pos,
          contigLengths
        )
        .map {
          case (nextRecordPos, delta) ⇒

            uncompressedBytes.seek(nextRecordPos)

            NextRecord(
              RecordStream(
                uncompressedBytes,
                header
              )
              .next()
              ._2,
              delta
            )
        }
      },
      flags
    )
}
