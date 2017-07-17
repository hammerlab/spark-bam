package org.hammerlab.bam.spark.compare

import caseapp.{ ExtraName ⇒ O }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE
import org.hammerlab.app.{ App, SparkApp }
import org.hammerlab.bam.spark.{ CanCompareSplits, SplitsArgs }
import org.hammerlab.bytes.Bytes
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.io.{ Printer, SampleSize }
import org.hammerlab.paths.Path
import Printer._

case class Opts(@O("f") bamsFile: Path,
                @O("l") printLimit: SampleSize = SampleSize(None),
                @O("m") splitSize: Option[Bytes] = None,
                @O("o") outPath: Option[Path] = None
               )
  extends SplitsArgs

object Main
  extends App[Opts]
    with SparkApp[Opts]
    with CanCompareSplits {

  override def run(opts: Opts, args: Seq[String]): Unit = {

    implicit val sampleSize = opts.printLimit
    implicit val printer = Printer(opts.outPath)

    val lines =
      opts
        .bamsFile
        .lines
        .map(_.trim)
        .filter(_.nonEmpty)
        .toVector

    val numBams = lines.length

    implicit val splitSize: MaxSplitSize = opts.splitSize

    ctx.setLong(SPLIT_MAXSIZE, splitSize)

    val confBroadcast = sc.broadcast(ctx)

    val pathResults =
      sc
        .parallelize(
          lines,
          numSlices = numBams
        )
        .map {
          bamPathStr ⇒
            val bamPath = Path(bamPathStr)

            implicit val conf = confBroadcast.value

            bamPath →
              getPathResult(bamPath)(splitSize, conf)
        }

    import org.hammerlab.math.MonoidSyntax._

    val (
      numSparkBamSplits,
      numHadoopBamSplits,
      sparkOnlySplits,
      hadoopOnlySplits
    ) =
      pathResults
        .values
        .map {
          case Result(numSparkSplits, numHadoopSplits, _, numSparkOnlySplits, numHadoopOnlySplits) ⇒
            (numSparkSplits, numHadoopSplits, numSparkOnlySplits, numHadoopOnlySplits)
        }
        .reduce { _ |+| _ }

    val diffs =
      pathResults
        .filter(_._2.diffs.nonEmpty)
        .collect

    if (diffs.isEmpty)
      println(s"All $numBams BAMs' splits (totals: $numSparkBamSplits, $numHadoopBamSplits) matched!")
    else {
      echo(
        s"${diffs.length} of $numBams BAMs' splits didn't match (totals: $numSparkBamSplits, $numHadoopBamSplits; $sparkOnlySplits, $hadoopOnlySplits unmatched):",
        ""
      )
      diffs.foreach {
        case (
          path,
          Result(
            numSparkSplits,
            numHadoopSplits,
            diffs,
            numSparkOnlySplits,
            numHadoopOnlySplits
          )
        ) ⇒
          val totalsMsg =
            s"totals: $numSparkSplits, $numHadoopSplits; mismatched: $numSparkOnlySplits, $numHadoopOnlySplits"

          print(
            diffs
              .map {
                case Left(ours) ⇒
                  s"\t\t$ours"
                case Right(theirs) ⇒
                  s"\t\t\t$theirs"
              },
            s"\t${path.basename}: ${diffs.length} splits differ ($totalsMsg):",
            n ⇒ s"\t${path.basename}: first $n of ${diffs.length} splits that differ ($totalsMsg):"
          )
      }
      echo("")
    }
  }
}
