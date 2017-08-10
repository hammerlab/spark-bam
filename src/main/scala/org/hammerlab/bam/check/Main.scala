package org.hammerlab.bam.check

import cats.syntax.all._
import cats.Show.show
import caseapp.{ ExtraName ⇒ O }
import htsjdk.samtools.SAMRecord
import org.apache.log4j.Level.WARN
import org.apache.log4j.Logger.getRootLogger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.app.{ SparkPathApp, SparkPathAppArgs }
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bam.iterator.RecordStream
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.spark.FindRecordStart
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ FindBlockStart, PosIterator, SeekableUncompressedBytes }
import org.hammerlab.bytes.Bytes
import org.hammerlab.hadoop.Configuration
import org.hammerlab.io.Printer._
import org.hammerlab.io.SampleSize
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.magic.rdd.SampleRDD.sample
import org.hammerlab.magic.rdd.partitions.OrderedRepartitionRDD._
import org.hammerlab.magic.rdd.size._
import org.hammerlab.math.ceil
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
 * @param eager if set, run [[org.hammerlab.bam.check.eager.Run]], which marks a position as "negative" and returns as
 *              soon as any check fails. Default: [[org.hammerlab.bam.check.full.Run]], which performs as many checks as
 *              possible and aggregates statistics about how many times each check participates in ruling out a given
 *              position.
 */
case class Args(@O("e") eager: Boolean = false,
                @O("f") full: Boolean = false,
                @O("g") bgzfBlockHeadersToCheck: Int = 5,
                @O("i") blocksPerPartition: Int = 20,
                @O("k") blocks: Option[Path] = None,
                @O("l") printLimit: SampleSize = SampleSize(None),
                @transient @O("m") splitSize: Option[Bytes] = None,
                @O("n") numBlocks: Option[Int] = None,
                @O("o") out: Option[Path] = None,
                @O("q") resultsPerPartition: Int = 1000000,
                @O("r") records: Option[Path] = None,
                @O("s") seqdoop: Boolean = false,
                @O("w") blocksWhitelist: Option[String] = None,
                warn: Boolean = false)
  extends SparkPathAppArgs

object Main
  extends SparkPathApp[Args](classOf[Registrar]) {

  /**
   * Entry-point delegated to by [[caseapp]]'s [[main]]; delegates to a [[Run]] implementation indicated by
   * [[Args.eager]].
   */
  override def run(args: Args): Unit = {

    if (args.warn)
      getRootLogger.setLevel(WARN)

    val runs: List[Run[_, _]] =
      (
        (if (args.eager) Some(eager.Run) else None).toList ::
          (if (args.full) Some(full.Run) else None).toList ::
          (if (args.seqdoop) Some(seqdoop.Run) else None).toList ::
          Nil
      )
      .flatten

    runs match {
      case Nil ⇒
        throw new IllegalArgumentException(
          "Provide at least one of '-e' (\"eager\" checker), '-f' (\"full\" checker), '-s' (\"seqdoop\" checker)"
        )

      case run :: Nil ⇒
        run(args).prettyPrint

      case eager.Run :: seqdoop.Run :: Nil ⇒

        val header = Header(path)
        val headerBroadcast = sc.broadcast(header)

        val contigLengthsBroadcast = sc.broadcast(header.contigLengths)

        val calls =
          compare(
            args,
            eager.Run,
            seqdoop.Run,
            contigLengthsBroadcast
          )

        val  numTruePositives = sc.longAccumulator("truePositives")
        val  numTrueNegatives = sc.longAccumulator("trueNegatives")
        val numFalsePositives = sc.longAccumulator("falsePositives")
        val numFalseNegatives = sc.longAccumulator("falseNegatives")

        val differingCalls = {
          val originalDifferingCalls =
            calls
              .filter {
                case (_, (eager, seqdoop)) ⇒
                  (eager, seqdoop) match {
                    case ( true,  true) ⇒
                      numTruePositives.add(1)
                      false
                    case ( true, false) ⇒
                      numFalseNegatives.add(1)
                      true
                    case (false,  true) ⇒
                      numFalsePositives.add(1)
                      true
                    case (false, false) ⇒
                      numTrueNegatives.add(1)
                      false
                  }
              }
              .map { case (pos, (eager, _)) ⇒ pos → eager }
              .setName("originalDifferingCalls")
              .cache

          val numDifferingCalls = originalDifferingCalls.size

          originalDifferingCalls
            .orderedRepartition(
              ceil(
                numDifferingCalls,
                args.resultsPerPartition
              )
              .toInt
            )
            .setName("repartitioned-differing-calls")
            .cache
        }

        val numEagerOnly = numFalseNegatives.value
        val numSeqdoopOnly = numFalsePositives.value
        val numDifferingCalls = numEagerOnly + numSeqdoopOnly

        val numReads = numTruePositives.value + numFalseNegatives.value

        val totalCalls =
          numReads +
            numTrueNegatives.value +
            numFalsePositives.value

        val compressedSize = path.size
        val compressionRatio = totalCalls.toDouble / compressedSize

        val seqdoops = differingCalls.filter(!_._2).keys
        val eagers = differingCalls.filter(_._2).keys

        val seqdoopsWithNextRecords =
          seqdoops
            .mapPartitions {
              it ⇒
                val ch = SeekableUncompressedBytes(path)
                val contigLengths = contigLengthsBroadcast.value
                val fullChecker = full.Checker(ch, contigLengths)
                it
                  .map {
                    pos ⇒
                      val (nextRecordPos, distance) =
                        FindRecordStart.withDelta(
                          path,
                          ch,
                          pos,
                          contigLengths
                        )

                      val allChecks = fullChecker(pos).get

                      ch.seek(nextRecordPos)

                      val nextRecord =
                        RecordStream(
                          ch,
                          headerBroadcast.value
                        )
                        .next()
                        ._2

                      pos →
                        (
                          nextRecordPos,
                          distance,
                          nextRecord,
                          allChecks
                        )
                  }
                  .finish(ch.close())
            }

        echo(
          s"$totalCalls uncompressed positions",
          s"${Bytes.format(compressedSize)} compressed",
          "Compression ratio: %.2f".format(compressionRatio),
          s"$numReads reads"
        )

        implicit val showRecord =
          show[SAMRecord] {
            record ⇒
              record.toString() +
                (
                  if (record.getReadUnmappedFlag && record.getStart >= 0)
                    s" (placed at ${header.contigLengths(record.getReferenceIndex)._1}:${record.getStart})"
                  else
                    ""
                )
          }

        import cats.implicits._

        def printSeqdoopOnly(): Unit = {
          val sampledPositions =
            sample(
              seqdoopsWithNextRecords
                .map {
                  case (pos, (_, delta, record, flags)) ⇒
                    show"$pos:\t$delta before $record. Failing checks: $flags"
                },
              numSeqdoopOnly
            )

          print(
            sampledPositions,
            numDifferingCalls,
            s"$numSeqdoopOnly seqdoop-only calls:",
            n ⇒ s"$n of $numSeqdoopOnly seqdoop-only calls:"
          )
        }

        def printEagers(): Unit = {
          print(
            sample(
              eagers,
              numEagerOnly
            ),
            numEagerOnly,
            s"$numEagerOnly eager-only calls:",
            n ⇒ s"$n of $numEagerOnly eager-only calls:"
          )
        }

        (numSeqdoopOnly > 0, numEagerOnly > 0) match {
          case (false, false) ⇒
            echo("All calls matched!")
          case (seqdoops, eagers) ⇒
            echo(s"$numSeqdoopOnly seqdoop-only calls, $numEagerOnly eager-only calls")
            if (seqdoops)
              printSeqdoopOnly()

            if (eagers)
              printEagers()
        }

      case _ ⇒
        throw new IllegalArgumentException(
          s"Invalid set of checkers: ${runs.mkString(",")}"
        )
    }
  }

  /**
   * Main CLI entry point: build a [[Result]] and print some statistics about it.
   */
  def compare(args: Args,
              run1: Run[Boolean, simple.PosResult],
              run2: Run[Boolean, simple.PosResult],
              contigLengths: Broadcast[ContigLengths])(
      implicit path: Path
  ): RDD[(Pos, (Boolean, Boolean))] = {
    implicit val conf: Configuration = sc.hadoopConfiguration

    val (_, blocks, _) = Blocks(args)

    blocks
      .mapPartitions {
        blocks ⇒
          val checker1 =
            run1.makeChecker(
              path,
              contigLengths.value
            )

          val checker2 =
            run2.makeChecker(
              path,
              contigLengths.value
            )

          blocks
            .flatMap(PosIterator(_))
            .map {
              pos ⇒
                pos →
                  (
                    checker1(pos),
                    checker2(pos)
                  )
            }
            .finish({
              checker1.close()
              checker2.close()
            })
      }
  }

}

case class MissingCall(pos: Pos,
                       eagerCall: Option[Boolean],
                       seqdoopCall: Option[Boolean])
  extends Exception(
    s"$pos: eager $eagerCall, seqdoop $seqdoopCall"
  )
