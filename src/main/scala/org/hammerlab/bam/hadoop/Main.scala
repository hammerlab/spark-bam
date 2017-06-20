package org.hammerlab.bam.hadoop

import java.io.PrintStream

import caseapp.{ CaseApp, RemainingArgs, ExtraName ⇒ O }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.JobID
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.spark.SparkContext
import org.hammerlab.parallel.threads
import org.hammerlab.bam.hadoop.LoadBam._
import org.hammerlab.hadoop.MaxSplitSize
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.parallel.spark
import org.hammerlab.timing.Timer.time
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit }

import scala.collection.JavaConverters._

case class JW(conf: Configuration) {
  val job = org.apache.hadoop.mapreduce.Job.getInstance(conf)
}

case class Args(@O("n") numWorkers: Option[Int],
                @O("u") useSeqdoop: Boolean = false,
                @O("g") gsBuffer: Option[Int] = None,
                @O("s") maxSplitSize: Option[Long] = None,
                @O("o") outFile: Option[String] = None)

object Main extends CaseApp[Args] {

  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {
    if (remainingArgs.remainingArgs.size != 1) {
      throw new IllegalArgumentException(s"Exactly one argument (a BAM file path) is required")
    }

    val path = new Path(remainingArgs.remainingArgs.head)
    implicit val conf = new Configuration

    if (!args.useSeqdoop) {
      implicit val sc = new SparkContext()

      val parallelizer =
        args.numWorkers match {
          case Some(numWorkers) ⇒
            threads.Config(numWorkers)
          case _ ⇒
            spark.Config()
        }

      implicit val config =
        Config(
          parallelizer = parallelizer,
          maxSplitSize = MaxSplitSize(args.maxSplitSize)
        )

      val reads = sc.loadBam(path)

      val partitionSizes = reads.partitionSizes

      println("Partition sizes:")
      println(
        partitionSizes
          .grouped(10)
          .map(
            _
              .map("% 5d".format(_))
              .mkString("")
          )
          .mkString("\n")
      )
    } else {

      args.gsBuffer match {
        case Some(gsBuffer) ⇒
          conf.setInt("fs.gs.io.buffersize", gsBuffer)
        case None ⇒
      }

      val ifmt = new BAMInputFormat

      val job = org.apache.hadoop.mapreduce.Job.getInstance(conf)
      val jobConf = job.getConfiguration

      val jobID = new JobID("get-splits", 1)
      val jc = new JobContextImpl(jobConf, jobID)

      FileInputFormat.setInputPaths(job, path)

      val splits = time("get splits") {
        ifmt.getSplits(jc)
      }

      val pw =
        args.outFile match {
          case Some(outFile) ⇒
            new PrintStream(outFile)
          case None ⇒
            System.out
        }

      pw.println(
        splits
          .asScala
          .map(
            split ⇒
              split.asInstanceOf[FileVirtualSplit]: Split
          )
          .map(split ⇒ s"${split.start}-${split.end}")
          .mkString("\t", "\n\t", "\n")
      )
    }
  }
}
