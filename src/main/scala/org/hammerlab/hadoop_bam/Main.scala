package org.hammerlab.hadoop_bam

import java.io.PrintStream

import caseapp._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.JobID
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.hammerlab.hadoop_bam.InputFormat.NUM_GET_SPLITS_WORKERS_KEY
import org.seqdoop.hadoop_bam.FileVirtualSplit

import scala.collection.JavaConverters._

case class JW(conf: Configuration) {
  val job = org.apache.hadoop.mapreduce.Job.getInstance(conf)
}

case class Args(@ExtraName("n") numWorkers: Option[Int],
                @ExtraName("u") useSeqdoop: Boolean = false,
                @ExtraName("g") gsBuffer: Option[Int] = None,
                @ExtraName("o") outFile: Option[String] = None)

object Main extends CaseApp[Args] {

  def time[T](fn: => T): T = time("")(fn)

  def time[T](msg: ⇒ String)(fn: => T): T = {
    val before = System.currentTimeMillis
    val res = fn
    val after = System.currentTimeMillis
    println(s"$msg:\t${after-before}ms")
    res
  }

  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {
    if (remainingArgs.remainingArgs.size != 1) {
      throw new IllegalArgumentException(s"Exactly one argument (a BAM file path) is required")
    }

    val path = new Path(remainingArgs.remainingArgs.head)
    val conf = new Configuration

    args.gsBuffer match {
      case Some(gsBuffer) ⇒
        conf.setInt("fs.gs.io.buffersize", gsBuffer)
      case None ⇒
    }

    val ifmt =
      if (args.useSeqdoop)
        new org.seqdoop.hadoop_bam.BAMInputFormat
      else
        new InputFormat

    val job = org.apache.hadoop.mapreduce.Job.getInstance(conf)
    val jobConf = job.getConfiguration

    args.numWorkers match {
      case Some(numWorkers) ⇒
        jobConf.setInt(NUM_GET_SPLITS_WORKERS_KEY, numWorkers)
      case None ⇒
    }

    val jobID = new JobID("get-splits", 1)
    val jc = new JobContextImpl(jobConf, jobID)

    FileInputFormat.setInputPaths(job, path)

    val splits = time("get splits") { ifmt.getSplits(jc) }

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
        .map(split ⇒
          if (args.useSeqdoop)
            split.asInstanceOf[FileVirtualSplit]: VirtualSplit
          else
            split.asInstanceOf[VirtualSplit]
        )
        .map(split ⇒ s"${split.start}-${split.end}")
        .mkString("\t", "\n\t", "\n")
    )
  }
}
