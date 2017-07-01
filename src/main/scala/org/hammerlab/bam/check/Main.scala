package org.hammerlab.bam.check

import caseapp.core.ContextArgParser
import caseapp.{ ExtraName ⇒ O }
import grizzled.slf4j.Logging
import org.hammerlab.SparkApp
import org.hammerlab.bgzf.Pos
import org.hammerlab.io.Printer._
import org.hammerlab.io.{ Printer, SampleSize }
import org.hammerlab.magic.rdd.SampleRDD.sample
import org.hammerlab.paths.Path
import org.hammerlab.spark.Context

/**
 * CLI for [[Main]]: check every (bgzf-decompressed) byte-position in a BAM file with a [[Checker]] and compare the
 * results to the true read-start positions.
 *
 * - Takes one argument: the path to a BAM file.
 * - Requires that BAM to have been indexed prior to running by [[org.hammerlab.bgzf.index.IndexBlocks]] and
 *   [[org.hammerlab.bam.index.IndexRecords]].
 *
 * @param blocksFile file with bgzf-block-start positions as output by [[org.hammerlab.bgzf.index.IndexBlocks]]
 * @param recordsFile file with BAM-record-start positions as output by [[org.hammerlab.bam.index.IndexRecords]]
 * @param numBlocks if set, only check the first [[numBlocks]] bgzf blocks of
 * @param blocksWhitelist if set, only process the bgzf blocks at these positions (comma-seperated)
 * @param blocksPerPartition process this many blocks in each partition
 * @param eagerChecker if set, run [[org.hammerlab.bam.check.eager.Run]], which marks a position as "negative" and
 *                     returns as soon as any check fails. Default: [[org.hammerlab.bam.check.full.Run]], which performs
 *                     as many checks as possible and aggregates statistics about how many times each check participates
 *                     in ruling out a given position.
 */
case class Args(@O("e") eagerChecker: Boolean = false,
                @O("f") fullChecker: Boolean = false,
                @O("i") blocksPerPartition: Int = 20,
                @O("k") blocksFile: Option[Path] = None,
                @O("m") samplesToPrint: SampleSize = SampleSize(None),
                @O("n") numBlocks: Option[Int] = None,
                @O("o") outputPath: Option[Path] = None,
                @O("q") resultPositionsPerPartition: Int = 1000000,
                @O("r") recordsFile: Option[Path] = None,
                @O("s") seqdoopChecker: Boolean = false,
                @O("w") blocksWhitelist: Option[String] = None)

object Args {
  implicitly[ContextArgParser[Context, Path]]
  implicitly[ContextArgParser[Context, SampleSize]]
}

object Main
  extends SparkApp[Args]
    with Logging {

  /**
   * Entry-point delegated to by [[caseapp]]'s [[main]]; delegates to a [[Run]] implementation indicated by
   * [[Args.eagerChecker]].
   */
  override def run(args: Args, remainingArgs: Seq[String]): Unit = {
    if (remainingArgs.size != 1) {
      throw new IllegalArgumentException(
        s"Exactly one argument (a BAM file path) is required"
      )
    }

    implicit val path = Path(remainingArgs.head)

    val runs: List[Run[_, _, _ <: Result[_]]] =
      (
        (if (args.eagerChecker) Some(eager.Run) else None).toList ::
          (if (args.fullChecker) Some(full.Run) else None).toList ::
          (if (args.seqdoopChecker) Some(seqdoop.Run) else None).toList ::
          Nil
      )
      .flatten

    implicit val printer = Printer(args.outputPath)
    implicit val sampleSize = args.samplesToPrint

    runs match {
      case Nil ⇒
        throw new IllegalArgumentException(
          "Provide at least one of '-e' (\"eager\" checker), '-f' (\"full\" checker), '-s' (\"seqdoop\" checker)"
        )

      case run :: Nil ⇒
        run(args).prettyPrint

      case eager.Run :: seqdoop.Run :: Nil ⇒
        val (eagerCalls, _) = eager.Run.getCalls(args)
        val (seqdoopCalls, _) = seqdoop.Run.getCalls(args)

        val differingCalls =
          eagerCalls
            .fullOuterJoin(seqdoopCalls)
            .flatMap {
              case (pos, (Some(eagerCall), Some(seqdoopCall))) ⇒
                if (eagerCall != seqdoopCall)
                  Some(pos → eagerCall)
                else
                  None
              case (pos, (eagerCall, seqdoopCall)) ⇒
                throw MissingCall(pos, eagerCall, seqdoopCall)
            }

        val counts =
          differingCalls
            .map(_._2 → 1L)
            .reduceByKey(_ + _)
            .collectAsMap()
            .toMap

        val numEagerOnly = counts.getOrElse(true, 0L)
        val numSeqdoopOnly = counts.getOrElse(false, 0L)

        def printCalls(filterTo: Boolean,
                       num: Long,
                       header: String,
                       truncatedHeader: Int ⇒ String): Unit = {
          val positions = differingCalls.filter(_._2 == filterTo).keys
          val sampledPositions = sample(positions, num).sorted
          printSamples(
            sampledPositions,
            num,
            header,
            truncatedHeader
          )
        }

        def printEagerOnly(): Unit =
          printCalls(
            true,
            numEagerOnly,
            "Eager-only calls:",
            n ⇒ s"First $n eager-only calls:"
          )

        def printSeqdoopOnly(): Unit =
          printCalls(
            false,
            numSeqdoopOnly,
            "Seqdoop-only calls:",
            n ⇒ s"First $n seqdoop-only calls:"
          )

        (numEagerOnly > 0, numSeqdoopOnly > 0) match {
          case (true, true) ⇒
            printEagerOnly()
            print("")
            printSeqdoopOnly()
          case (true, false) ⇒
            printEagerOnly()
          case (false, true) ⇒
            printSeqdoopOnly()
          case (false, false) ⇒
            print("All calls matched!")
        }

      case _ ⇒
        throw new IllegalArgumentException(
          s"Invalid set of checkers: ${runs.mkString(",")}"
        )
    }
  }
}

case class MissingCall(pos: Pos,
                       eagerCall: Option[Boolean],
                       seqdoopCall: Option[Boolean])
  extends Exception(
    s"$pos: eager $eagerCall, seqdoop $seqdoopCall"
  )
