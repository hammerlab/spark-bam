package org.hammerlab.bam.check

import caseapp.{ ExtraName ⇒ O, _ }
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.hammerlab.bam.check.full.error.Registrar
import org.hammerlab.bgzf.Pos
import org.hammerlab.io.Printer._
import org.hammerlab.io.{ Printer, SampleSize }
import org.hammerlab.spark.Conf
import org.hammerlab.spark.SampleRDD.sample

/**
 * CLI for [[Main]]: check every (bgzf-decompressed) byte-position in a BAM file with a [[Checker]] and compare the
 * results to the true read-start positions.
 *
 * Requires the BAM to have been indexed prior to running by [[org.hammerlab.bgzf.index.IndexBlocks]] and
 * [[org.hammerlab.bam.index.IndexRecords]].
 *
 * @param bamFile BAM file to check all (uncompressed) positions of
 * @param blocksFile file with bgzf-block-start positions as output by [[org.hammerlab.bgzf.index.IndexBlocks]]
 * @param recordsFile file with BAM-record-start positions as output by [[org.hammerlab.bam.index.IndexRecords]]
 * @param numBlocks if set, only check the first [[numBlocks]] bgzf blocks of [[bamFile]]
 * @param blocksWhitelist if set, only process the bgzf blocks at these positions (comma-seperated)
 * @param blocksPerPartition process this many blocks in each partition
 * @param eagerChecker if set, run [[org.hammerlab.bam.check.eager.Run]], which marks a position as "negative" and
 *                     returns as soon as any check fails. Default: [[org.hammerlab.bam.check.full.Run]], which performs
 *                     as many checks as possible and aggregates statistics about how many times each check participates
 *                     in ruling out a given position.
 */
case class Args(@O("b") bamFile: String,
                @O("e") eagerChecker: Boolean = false,
                @O("f") fullChecker: Boolean = false,
                @O("i") blocksPerPartition: Int = 20,
                @O("k") blocksFile: Option[String] = None,
                @O("m") samplesToPrint: Option[Int] = None,
                @O("n") numBlocks: Option[Int] = None,
                @O("o") outputPath: Option[String] = None,
                @O("p") propertiesFiles: String = "",
                @O("q") resultPositionsPerPartition: Int = 1000000,
                @O("r") recordsFile: Option[String] = None,
                @O("s") seqdoopChecker: Boolean = false,
                @O("w") blocksWhitelist: Option[String] = None)

object Main
  extends CaseApp[Args]
    with Logging {

  /**
   * Entry-point delegated to by [[caseapp]]'s [[main]]; delegates to a [[Run]] implementation indicated by
   * [[Args.eagerChecker]].
   */
  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {

    val sparkConf = Conf(args.propertiesFiles)

    sparkConf.setIfMissing(
      "spark.kryo.registrator",
      classOf[Registrar].getName
    )

    sparkConf.setIfMissing(
      "spark.kryo.registrationRequired",
      "true"
    )

    implicit val sc = new SparkContext(sparkConf)

    val runs: List[Run[_, _, _ <: Result[_]]] =
      (
        (if (args.eagerChecker) Some(eager.Run) else None).toList ::
          (if (args.fullChecker) Some(full.Run) else None).toList ::
          (if (args.seqdoopChecker) Some(seqdoop.Run) else None).toList ::
          Nil
      )
      .flatten

    implicit val printer = Printer(args.outputPath)
    implicit val sampleSize = SampleSize(args.samplesToPrint)

    runs match {
      case Nil ⇒
        throw new IllegalArgumentException(
          "Provide at least one of '-e' (\"eager\" checker), '-f' (\"full\" checker), '-s' (\"seqdoop\" checker)"
        )

      case run :: Nil ⇒
        run(sc, args).prettyPrint

      case eager.Run :: seqdoop.Run :: Nil ⇒
        val (eagerCalls, _) = eager.Run.getCalls(sc, args)
        val (seqdoopCalls, _) = seqdoop.Run.getCalls(sc, args)

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
