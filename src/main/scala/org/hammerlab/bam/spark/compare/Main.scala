package org.hammerlab.bam.spark.compare

import caseapp.{ ExtraName ⇒ O }
import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE
import org.hammerlab.app.{ App, SparkApp }
import org.hammerlab.bam.spark.{ CanCompareSplits, SplitsArgs }
import org.hammerlab.bytes.Bytes
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.io.Printer._
import org.hammerlab.io.{ Printer, SampleSize }
import org.hammerlab.iterator.SliceIterator._
import org.hammerlab.kryo.serializeAs
import org.hammerlab.paths.Path

case class Opts(@O("f") bamsFile: Path,
                @O("l") printLimit: SampleSize = SampleSize(None),
                @O("m") splitSize: Option[Bytes] = None,
                @O("n") filesLimit: Option[Int] = None,
                @O("o") outPath: Option[Path] = None,
                @O("s") startOffset: Option[Int] = None
               )
  extends SplitsArgs

object Main
  extends App[Opts]
    with SparkApp[Opts]
    with CanCompareSplits {

  override def _run(opts: Opts, args: Seq[String]): Unit = {

    implicit val sampleSize = opts.printLimit
    implicit val printer = Printer(opts.outPath)

    val lines =
      opts
        .bamsFile
        .lines
        .map(_.trim)
        .filter(_.nonEmpty)
        .sliceOpt(opts.startOffset, opts.filesLimit)
        .toVector

    val numBams = lines.length

    implicit val splitSize: MaxSplitSize = opts.splitSize

    ctx.setLong(SPLIT_MAXSIZE, splitSize)

    val conf: Configuration = sc.hadoopConfiguration
    val confBroadcast = sc.broadcast(conf)

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
        .cache

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
