package org.hammerlab.bam.spark.compare

import caseapp.{ AppName, ProgName, Recurse, ExtraName ⇒ O, HelpMessage ⇒ M }
import cats.implicits.catsKernelStdGroupForInt
import cats.syntax.all._
import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE
import org.hammerlab.app.{ SparkPathApp, SparkPathAppArgs }
import org.hammerlab.args.{ FindBlockArgs, FindReadArgs, OutputArgs, SplitSize }
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.spark.CanCompareSplits
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.io.Printer._
import org.hammerlab.iterator.SliceIterator._
import org.hammerlab.kryo.serializeAs
import org.hammerlab.paths.Path
import org.hammerlab.types.Monoid._

@AppName("Compare splits computed from many BAM files listed in a given file")
@ProgName("… org.hammerlab.bam.spark.compare")
case class Opts(@Recurse output: OutputArgs,
                @Recurse splitSizeArgs: SplitSize.Args,
                @Recurse findReadArgs: FindReadArgs,
                @Recurse findBlockArgs: FindBlockArgs,

                @O("n")
                @M("Only process this many files")
                filesLimit: Option[Int] = None,

                @O("s")
                @M("Start from this offset into the file")
                startOffset: Option[Int] = None
               )
  extends SparkPathAppArgs

object Main
  extends SparkPathApp[Opts](classOf[Registrar])
    with CanCompareSplits {

  override protected def run(opts: Opts): Unit = {

    val lines =
      path
        .lines
        .map(_.trim)
        .filter(_.nonEmpty)
        .sliceOpt(opts.startOffset, opts.filesLimit)
        .toVector

    val numBams = lines.length

    implicit val splitSize: MaxSplitSize = opts.splitSizeArgs.maxSplitSize

    implicit val FindReadArgs(maxReadSize, readsToCheck) = opts.findReadArgs

    implicit val bgzfBlocksToCheck = opts.findBlockArgs.bgzfBlocksToCheck

    ctx.setLong(SPLIT_MAXSIZE, splitSize)

    val pathResults =
      sc
        .parallelize(
          lines,
          numSlices = numBams
        )
        .map {
          bamPathStr ⇒
            val bamPath = Path(bamPathStr)

            bamPath →
              getPathResult(bamPath)
        }
        .cache

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

  def register(kryo: Kryo): Unit = {
    /** A [[Configuration]] gets broadcast */
    Configuration.register(kryo)

    /** BAM-files are distributed as [[Path]]s which serialize as [[String]]s */
    kryo.register(
      classOf[Path],
      serializeAs[Path, String](_.toString, Path(_))
    )
    kryo.register(classOf[Array[String]])

    /** [[Result]]s get [[org.apache.spark.rdd.RDD.collect collected]] */
    kryo.register(classOf[Result])
  }
}
