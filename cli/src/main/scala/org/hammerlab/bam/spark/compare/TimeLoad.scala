package org.hammerlab.bam.spark.compare

import caseapp.Recurse
import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.hammerlab.args.SplitSize
import org.hammerlab.bam.spark.load
import org.hammerlab.bam.spark.LoadReads
import org.hammerlab.cli.app.{ SparkPathApp, SparkPathAppArgs }
import org.hammerlab.cli.args.OutputArgs
import org.hammerlab.exception.Error
import org.hammerlab.io.Printer.echo
import org.hammerlab.iterator.NextOptionIterator
import org.hammerlab.timing.Timer

case class TimeLoadArgs(@Recurse output: OutputArgs,
                        @Recurse splitSizeArgs: SplitSize.Args)
  extends SparkPathAppArgs

object TimeLoad
  extends SparkPathApp[TimeLoadArgs, load.Registrar]
    with Timer
    with LoadReads {

  override protected def run(args: TimeLoadArgs): Unit = {
    implicit val splitSizeArgs = args.splitSizeArgs
    implicit val splitSize = splitSizeArgs.maxSplitSize

    def firstReadNames(reads: RDD[SAMRecord]): Array[String] =
      reads
        .mapPartitions(
          _
            .nextOption
            .iterator
        )
         .map(_.getReadName)
         .collect

    val (sparkBamMS, sparkBamReads) =
      time {
        firstReadNames(sparkBamLoad)
      }

    try {
      val (hadoopBamMS, hadoopBamReads) =
        time {
          firstReadNames(hadoopBamLoad)
        }

      echo(
        s"spark-bam first-read collection time: $sparkBamMS",
        s"hadoop-bam first-read collection time: $hadoopBamMS",
        ""
      )

      val sparkBamNames = sparkBamReads.toSet
      val hadoopBamNames = hadoopBamReads.toSet

      val onlySparkBam = sparkBamNames.diff(hadoopBamNames)
      val onlyHadoopBam = hadoopBamNames.diff(sparkBamNames)

      if (onlySparkBam.nonEmpty)
        echo(
          "spark-bam returned partition-start reads:",
          onlySparkBam.mkString("\t", "\n\t", "\n")
        )

      if (onlyHadoopBam.nonEmpty)
        echo(
          "hadoop-bam returned partition-start reads:",
          onlyHadoopBam.mkString("\t", "\n\t", "\n")
        )

      if (onlySparkBam.isEmpty && onlyHadoopBam.isEmpty)
        echo(
          s"All ${sparkBamReads.length} partition-start reads matched"
        )
    } catch {
      case e: Throwable ⇒
        echo(
          s"spark-bam first-read collection time: $sparkBamMS",
          "",
          s"spark-bam collected ${sparkBamReads.length} partitions' first-reads",
          "hadoop-bam threw an exception:",
          Error(e)
        )
    }
  }
}

//object TimeLoad extends TimeLoad
