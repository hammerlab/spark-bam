package org.hammerlab.bam.spark.compare

import caseapp.{ Name ⇒ O, Recurse ⇒ R }
import hammerlab.bool._
import hammerlab.monoid._
import hammerlab.lines._
import hammerlab.print._
import hammerlab.show._
import org.hammerlab.args.SplitSize
import org.hammerlab.bam.spark.{ LoadReads, load }
import org.hammerlab.cli.app.Cmd
import org.hammerlab.cli.app.spark.PathApp
import org.hammerlab.cli.args.PrintLimitArgs
import org.hammerlab.exception.Error
import org.hammerlab.stats.{ Empty, Stats }
import org.hammerlab.timing.Timer
import spark_bam._

import scala.util.{ Failure, Success, Try }

object CountReads
  extends Cmd {

  case class Opts(@R printLimit: PrintLimitArgs,
                  @R splitSizeArgs: SplitSize.Args,
                  @O("s") sparkBamFirst: Boolean = false,
                  @O("n") numIterations: Int = 1)

  val main = Main(
    new PathApp(_, load.Registrar)
      with Timer
      with LoadReads {

      implicit val splitSizeArgs = opts.splitSizeArgs
      val splitSize = splitSizeArgs.maxSplitSize

      def runOnce(): Result = {
        lazy val sparkBam =
          Reads(
            time {
              sc.loadBam(path, splitSize).count
            }
          )

        if (opts.sparkBamFirst) {
          info("Running spark-bam first…")
          sparkBam
        }

        val hadoopBam =
          Try {
            Reads(
              time {
                hadoopBamLoad.count
              }
            )
          }

        Result(sparkBam, hadoopBam)
      }

      opts.numIterations match {
        case 1 ⇒ echo(runOnce())
        case n if n > 0 ⇒
          val results = Results((0 until opts.numIterations).map(_ ⇒ runOnce()))
          echo(results)
        case n ⇒
          throw new IllegalArgumentException(s"Invalid numIterations: $n")
      }
    }
  )

  case class Reads(timeMS: Long, numReads: Long)
  object Reads {
    def apply(t: (Long, Long)): Reads = Reads(t._1, t._2)
  }

  case class Result(sparkBam: Reads, hadoopBam: Try[Reads])

  import hammerlab.indent.implicits.tab

  object Result {
    implicit val showResult: ToLines[Result] =
      (t: Result) ⇒ {
        val Result(sparkBam, hadoopBam) = t
        Lines(
          s"spark-bam read-count time: ${sparkBam.timeMS}",
          hadoopBam match {
            case Success(hadoopBam) ⇒
              Lines(
                s"hadoop-bam read-count time: ${hadoopBam.timeMS}",
                "",
                if (sparkBam.numReads == hadoopBam.numReads)
                  s"Read counts matched: ${sparkBam.numReads}"
                else
                  s"Read counts mismatched: ${sparkBam.numReads} via spark-bam, ${hadoopBam.numReads} via hadoop-bam"
              )
            case Failure(e) ⇒
              Lines(
                "",
                s"spark-bam found ${sparkBam.numReads} reads, hadoop-bam threw exception:",
                Error(e)
              )
          }
        )
    }
  }

  case class Results(numRuns: Int,
                     failures: Seq[Throwable],
                     sparkBamTimes: Stats[Long, Int],
                     hadoopBamTimes: Stats[Long, Int],
                     sparkBamNumReads: Map[Long, Int],
                     hadoopBamNumReads: Map[Long, Int])

  object Results {
    def apply(results: Seq[Result]): Results = {
      val failures = results.collect { case Result(_, Failure(e)) ⇒ e }
      val sparkBamTimes = Stats(results.map { case Result(Reads(time, _), _) ⇒ time })
      val hadoopBamTimes = Stats(results.collect { case Result(_, Success(Reads(time, _))) ⇒ time })
      val sparkBamNumReads = results.map { case Result(Reads(_, numReads), _) ⇒ Map(numReads → 1) }.reduce(_ |+| _)
      val hadoopBamNumReads = results.map { case Result(_, Success(Reads(_, numReads))) ⇒ Map(numReads → 1) }.reduce(_ |+| _)

      Results(
        results.size,
        failures,
        sparkBamTimes,
        hadoopBamTimes,
        sparkBamNumReads,
        hadoopBamNumReads
      )
    }

    implicit def showReadsCountsMap(implicit p: Printer): Show[Map[Long, Int]] =
      Show {
        _
          .map {
            case (numReads, numRuns) ⇒
              s"\t$numReads ($numRuns runs)"
          }
          .mkString("\n")
      }

    implicit val showResults: ToLines[Results] =
      (t: Results) ⇒ {
        val Results(
          numRuns,
          failures,
          sparkBamTimes,
          hadoopBamTimes,
          sparkBamNumReads,
          hadoopBamNumReads
        ) = t

        Lines(

          failures.nonEmpty |
            Lines(
              s"hadoop-bam failed on ${failures.size} of $numRuns runs:",
              "",
              failures map {
                e ⇒
                  Lines(
                    Error(e),
                    ""
                  )
              }
            ),

          "spark-bam times (ms):",
          sparkBamTimes,
          "",

          hadoopBamTimes match {
            case Empty() ⇒ None
            case _ ⇒
              Lines(
                "hadoop-bam times (ms):",
                hadoopBamTimes,
                ""
              )
          },

          (sparkBamNumReads.size, hadoopBamNumReads.size) match {
            case (1, 1) ⇒
              val sparkBamReads = sparkBamNumReads.head
              val hadoopBamReads = hadoopBamNumReads.head
              if (sparkBamReads == hadoopBamReads)
                s"Read counts matched: ${sparkBamReads._1}"
              else if (sparkBamReads._1 == hadoopBamReads._1)
                s"${sparkBamReads._2} spark-bam runs matched ${hadoopBamReads._2} hadoop-bam runs: ${sparkBamReads._1}"
              else
                s"Read counts mismatched: ${sparkBamReads._1} via spark-bam, ${hadoopBamReads._1} via hadoop-bam"
            case (n, 0|1) if n > 1 ⇒
              Lines(
                "spark-bam read-counts:",
                sparkBamNumReads,
                (hadoopBamNumReads.size == 1) |
                  Lines(
                    "",
                    s"hadoop-bam: ${hadoopBamNumReads.head._1}"
                  )
              )
            case (1, _) ⇒
              Lines(
                s"spark-bam: ${sparkBamNumReads.head._1}",
                "",
                "hadoop-bam read-counts:",
                hadoopBamNumReads
              )
          }
        )
      }
  }
}
