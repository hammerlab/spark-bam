package org.hammerlab.bam.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.hadoop.mapreduce.{ InputSplit, JobID }
import org.hammerlab.bgzf.Pos
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit }

import scala.collection.JavaConverters._

class InputFormatTest
  extends Suite {

  {
    implicit val path: Path = "1k.bam"

    test("1k-1m") {
      checkSplits(1000000, 1)
    }

    test("1k-100k") {
      checkSplits(100000, 2)
    }

    test("1k-50k") {
      checkSplits(50000, 4)
    }

    test("1k-30k") {
      checkSplits(30000, 3)
    }

    test("1k-10k") {
      checkSplits(10000, 1)
    }
  }

  {
    implicit val path: Path = "5k.bam"

    test("5k-1m") {
      checkSplits(1000000, 1)
    }

    test("5k-100k") {
      checkSplits(100000, 11)
    }

    test("5k-50k") {
      checkSplits(50000, 21)
    }

    test("5k-30k") {
      checkSplits(30000, 16)
    }

    test("5k-10k") {
      checkSplits(10000, 1)
    }
  }

  def checkSplits(maxSplitSize: MaxSplitSize,
                  numSplits: Int)(implicit path: Path): Unit =
    checkSplits(
      maxSplitSize,
      Some(numSplits)
    )

  def checkSplits(maxSplitSize: MaxSplitSize,
                  numSplitsOpt: Option[Int] = None)(implicit path: Path): Unit = {

    implicit val mss = maxSplitSize
    val theirs = theirSplits
    val ours = ourSplits

    numSplitsOpt match {
      case Some(numSplits) ⇒
        (ours.size, theirs.size) should be((numSplits, numSplits))
      case None ⇒
        ours.size should be(theirs.size)
    }

    for {
      ((Split(_, ourStart, ourEnd, _), theirSplit), idx) ← ours.zip(theirs).zipWithIndex
      theirStart = Pos(theirSplit.getStartVirtualOffset)
      theirEnd = Pos(theirSplit.getEndVirtualOffset)
    } {
      withClue(s"split $idx:") {
        ourStart should be(theirStart)
        ourEnd should be(theirEnd)
      }
    }
  }

  implicit class MaxSplitSize(val size: Int)
  implicit def unwrapSplitSize(mss: MaxSplitSize): Int = mss.size

  def ourSplits(implicit
                path: Path,
                maxSplitSize: MaxSplitSize,
                extraConf: Map[String, String] = Map()): Seq[Split] =
    getSplits(new InputFormat)
      .map(_.asInstanceOf[Split])

  def theirSplits(implicit
                  path: Path,
                  maxSplitSize: MaxSplitSize,
                  extraConf: Map[String, String] = Map()): Seq[FileVirtualSplit] =
    getSplits(new BAMInputFormat)
      .map(_.asInstanceOf[FileVirtualSplit])

  def getSplits[K, V](ifmt: BAMInputFormat)(
      implicit
      path: Path,
      maxSplitSize: MaxSplitSize,
      extraConf: Map[String, String] = Map()): Seq[InputSplit] = {

    val conf = new Configuration

    for {
      (k, v) ← extraConf
    } {
      conf.set(k, v)
    }

    val job = org.apache.hadoop.mapreduce.Job.getInstance(conf)
    FileInputFormat.setInputPaths(job, path)

    val jobConf = job.getConfiguration
    jobConf.setInt(FileInputFormat.SPLIT_MAXSIZE, maxSplitSize)

    val jobID = new JobID("foo", 1)
    val jc = new JobContextImpl(jobConf, jobID)

    ifmt.getSplits(jc).asScala
  }

  implicit def makePath(pathStr: String): Path = new Path(File(pathStr))
}
