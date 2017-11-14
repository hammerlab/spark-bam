package org.hammerlab.bam.spark.compare

import caseapp.{ AppName, ProgName, Name ⇒ O, Recurse ⇒ R }
import org.hammerlab.args.{ FindBlockArgs, FindReadArgs, IntRanges, SplitSize }
import org.hammerlab.cli.app.Cmd
import org.hammerlab.cli.app.spark.PathApp
import org.hammerlab.cli.args.PrintLimitArgs
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.kryo._
import org.hammerlab.stats.Stats
import shapeless._

import scala.collection.mutable

object CompareSplits extends Cmd {

  @AppName("Compare splits computed from many BAM files listed in a given file")
  @ProgName("… org.hammerlab.bam.spark.compare")
  case class Opts(@R printLimit: PrintLimitArgs,
                  @R splitSizeArgs: SplitSize.Args,
                  @R findReadArgs: FindReadArgs,
                  @R findBlockArgs: FindBlockArgs,

                  @O("r")
                  fileRanges: Option[IntRanges] = None
  )

  val main = Main(
    args ⇒ new PathApp(args, Registrar) {

      val lines =
        path
          .lines
          .map(_.trim)
          .filter(_.nonEmpty)
          .zipWithIndex
          .collect {
            case (path, idx)
              if args.fileRanges.forall(_.contains(idx)) ⇒
              path
          }
          .toVector

      val numBams = lines.length

      implicit val splitSize: MaxSplitSize = opts.splitSizeArgs.maxSplitSize

      implicit val FindReadArgs(maxReadSize, readsToCheck) = opts.findReadArgs

      implicit val bgzfBlocksToCheck = opts.findBlockArgs.bgzfBlocksToCheck

      opts.splitSizeArgs.set

      val pathResults =
        new PathChecks(lines, numBams)
          .results

      import cats.implicits.catsKernelStdMonoidForVector
      import hammerlab.monoid._
      import hammerlab.show._

      val (
        (timingRatios: Seq[Double]) ::  // IntelliJ needs some help on the type inference here 🤷
        numSparkBamSplits ::
        numHadoopBamSplits ::
        sparkOnlySplits ::
        hadoopOnlySplits ::
        hadoopBamMS ::
        sparkBamMS ::
        HNil
      ) =
        pathResults
          .values
          .map {
            result ⇒
              // Create an HList with a Vector of timing-ratio Doubles followed by the Int fields from Result
              Vector(result.sparkBamMS.toDouble / result.hadoopBamMS) ::
                Result.gen.to(result).filter[Int]  // drop the differing-splits vector, leave+sum just the other (numeric) fields
          }
          .reduce { _ |+| _ }

      val diffs =
        pathResults
          .filter(_._2.diffs.nonEmpty)
          .collect

      implicit val showDouble: Show[Double] = show { "%.1f".format(_) }

      def printTimings(): Unit = {
        echo(
          "Total split-computation time:",
          s"\thadoop-bam:\t$hadoopBamMS",
          s"\tspark-bam:\t$sparkBamMS",
          ""
        )

        if (timingRatios.size > 1)
          echo(
            "Ratios:",
            Stats(timingRatios, onlySampleSorted = true),
            ""
          )
        else if (timingRatios.size == 1)
          echo(
            show"Ratio: ${timingRatios.head}",
            ""
          )
      }

      if (diffs.isEmpty) {
        echo(
          s"All $numBams BAMs' splits (totals: $numSparkBamSplits, $numHadoopBamSplits) matched!",
          ""
        )
        printTimings()
      } else {
        echo(
          s"${diffs.length} of $numBams BAMs' splits didn't match (totals: $numSparkBamSplits, $numHadoopBamSplits; $sparkOnlySplits, $hadoopOnlySplits unmatched)",
          ""
        )
        printTimings()
        diffs.foreach {
          case (
            path,
            Result(
              numSparkSplits,
              numHadoopSplits,
              diffs,
              numSparkOnlySplits,
              numHadoopOnlySplits,
              _,
              _
            )
          ) ⇒
            val totalsMsg =
              s"totals: $numSparkSplits, $numHadoopSplits; mismatched: $numSparkOnlySplits, $numHadoopOnlySplits"

            print(
              diffs
                .map {
                  case Left(ours) ⇒
                    show"\t$ours"
                  case Right(theirs) ⇒
                    show"\t\t$theirs"
                },
              s"\t${path.basename}: ${diffs.length} splits differ ($totalsMsg):",
              n ⇒ s"\t${path.basename}: first $n of ${diffs.length} splits that differ ($totalsMsg):"
            )
            echo("")
        }
      }
    }
  )

  /** Import this here to avoid conflict with [[shapeless.Path]] */
  import hammerlab.path.Path
  import org.hammerlab.bam.kryo.pathSerializer

  case class Registrar() extends spark.Registrar(
    cls[mutable.WrappedArray.ofRef[_]],
    cls[Path],      // collected
    cls[Result],    // collected
    cls[_ :: _],    // reduced
    HNil.getClass,
    cls[Result]
  )
}
