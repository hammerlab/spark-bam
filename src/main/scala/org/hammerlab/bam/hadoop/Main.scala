package org.hammerlab.bam.hadoop

import java.io.{ IOException, PrintStream }

import caseapp._
import htsjdk.samtools.seekablestream.ByteArraySeekableStream
import htsjdk.samtools.util.BlockCompressedInputStream
import htsjdk.samtools.{ BAMRecordCodec, SAMRecord }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.JobID
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.spark.SparkContext
import org.hammerlab.bam.hadoop.InputFormat.NUM_GET_SPLITS_WORKERS_KEY
import org.hammerlab.bam.hadoop.LoadBam._
import org.hammerlab.bgzf.Pos
import org.hammerlab.iterator.SimpleBufferedIterator
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.timing.Timer.time
import org.seqdoop.hadoop_bam.util.WrapSeekable
import org.seqdoop.hadoop_bam.{ FileVirtualSplit, LazyBAMRecordFactory }

import scala.collection.JavaConverters._

case class JW(conf: Configuration) {
  val job = org.apache.hadoop.mapreduce.Job.getInstance(conf)
}

case class Args(@ExtraName("n") numWorkers: Option[Int],
                @ExtraName("u") useSeqdoop: Boolean = false,
                @ExtraName("f") useForkedSeqdoop: Boolean = false,
                @ExtraName("g") gsBuffer: Option[Int] = None,
                @ExtraName("o") outFile: Option[String] = None)

object Main extends CaseApp[Args] {

  def readFrom(path: Path, conf: Configuration, pos: Pos): Iterator[SAMRecord] = {
    val sin = WrapSeekable.openPath(conf, path)
    sin.seek(pos.blockPos)
    val BLOCKS_NEEDED_FOR_GUESS = 3 * 0xffff + 0xfffe
    val arr = Array.fill[Byte](BLOCKS_NEEDED_FOR_GUESS)(0)
    if (sin.read(arr) < arr.length) {
      throw new IOException(s"Too few bytes read")
    }
    val in = new ByteArraySeekableStream(arr)
    val bgzf = new BlockCompressedInputStream(in)
    bgzf.setCheckCrcs(true)
    bgzf.seek(pos.offset)

    val bamCodec = new BAMRecordCodec(null, new LazyBAMRecordFactory)
    bamCodec.setInputStream(bgzf)
    new SimpleBufferedIterator[SAMRecord] {
      override protected def _advance: Option[SAMRecord] =
        Option(bamCodec.decode())
    }
  }

  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {
    if (remainingArgs.remainingArgs.size != 1) {
      throw new IllegalArgumentException(s"Exactly one argument (a BAM file path) is required")
    }

    val path = new Path(remainingArgs.remainingArgs.head)
    val conf = new Configuration

    if (!args.useSeqdoop && !args.useForkedSeqdoop) {
      val sc = new SparkContext()
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
          .map(split ⇒
            if (args.useSeqdoop)
              split.asInstanceOf[FileVirtualSplit]: Split
            else
              split.asInstanceOf[Split]
          )
          .map(split ⇒ s"${split.start}-${split.end}")
          .mkString("\t", "\n\t", "\n")
      )
    }
  }
}

import org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat, FileSplit }
import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, RecordReader, TaskAttemptContext }

import scala.collection.JavaConverters._
class TestInputFormat extends FileInputFormat {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Nothing, Nothing] = ???

  def fileSplits(job: JobContext): Vector[FileSplit] =
    super
      .getSplits(job)
      .asScala
      .map(_.asInstanceOf[FileSplit])
      .toVector
}
