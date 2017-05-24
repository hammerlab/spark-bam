package org.hammerlab.hadoop_bam

import java.io.{ IOException, PrintStream }

import caseapp._
import htsjdk.samtools.{ BAMRecordCodec, SAMRecord }
import htsjdk.samtools.seekablestream.ByteArraySeekableStream
import htsjdk.samtools.util.BlockCompressedInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, JobID, RecordReader, TaskAttemptContext }
import org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat, FileSplit }
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.hammerlab.hadoop_bam.InputFormat.NUM_GET_SPLITS_WORKERS_KEY
import org.hammerlab.hadoop_bam.bgzf.Pos
import org.hammerlab.iterator.SimpleBufferedIterator
import org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat, FileSplit }
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit, LazyBAMRecordFactory }
import org.seqdoop.hadoop_bam.util.WrapSeekable

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
            split.asInstanceOf[FileVirtualSplit]: Split
          else
            split.asInstanceOf[Split]
        )
        .map(split ⇒ s"${split.start}-${split.end}")
        .mkString("\t", "\n\t", "\n")
    )
  }
}

import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, JobID, RecordReader, TaskAttemptContext }
import org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat, FileSplit }
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
