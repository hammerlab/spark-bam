package org.hammerlab.bam.spark.compare

import caseapp.Recurse
import htsjdk.samtools.SAMRecord
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.hammerlab.args.SplitSize
import org.hammerlab.bam.spark._
import org.hammerlab.cli.app
import org.hammerlab.cli.app.{ Args, Cmd }
import org.hammerlab.cli.app.spark.PathApp
import org.hammerlab.cli.args.PrintLimitArgs
import org.hammerlab.exception.Error
import org.hammerlab.io.Printer.echo
import org.hammerlab.iterator.NextOptionIterator
import org.hammerlab.timing.Timer
import org.seqdoop.hadoop_bam.{ BAMInputFormat, SAMRecordWritable }

object TimeLoad extends Cmd {
  case class Opts(@Recurse printLimit: PrintLimitArgs,
                  @Recurse splitSizeArgs: SplitSize.Args)

//  object Main extends app.Main(App)

  val main = Main(
    args ⇒ new PathApp(args, load.Registrar)
      with Timer
      with LoadReads {

      implicit val splitSizeArgs = args.splitSizeArgs
      val splitSize = splitSizeArgs.maxSplitSize

      def firstReadNames(reads: RDD[SAMRecord]): Array[String] =
        reads
          .mapPartitions(
            _
              .nextOption
              .iterator
          )
           .map(_.getReadName)
           .collect

      lazy val (sparkBamMS, sparkBamReads) =
        time {
          firstReadNames(sc.loadBam(path, splitSize))
        }

      splitSizeArgs.set

      try {
        val (hadoopBamMS, hadoopBamReads) =
          time {
            val rdd =
              sc.newAPIHadoopFile(
                path.toString(),
                classOf[BAMInputFormat],
                classOf[LongWritable],
                classOf[SAMRecordWritable]
              )

            val reads =
              rdd
                .values
                .map(_.get())

            firstReadNames(reads)
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
            s"spark-bam returned ${onlySparkBam.size} unmatched partition-start reads:",
            onlySparkBam.mkString("\t", "\n\t", "\n")
          )

        if (onlyHadoopBam.nonEmpty)
          echo(
            s"hadoop-bam returned ${onlyHadoopBam.size} unmatched partition-start reads:",
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
  )
}
