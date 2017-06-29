package org.hammerlab.bam.spark

import caseapp.{ CaseApp, RemainingArgs, ExtraName ⇒ O }
import grizzled.slf4j.Logging
import org.hammerlab.hadoop.{ Configuration, MaxSplitSize, Path }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.{ SPLIT_MAXSIZE, setInputPaths }
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.hadoop.mapreduce.{ Job, JobID }
import org.apache.spark.SparkContext
import org.hammerlab.bam.spark.LoadBam._
import org.hammerlab.bgzf.Pos
import org.hammerlab.io.{ Printer, SampleSize, Size }
import org.hammerlab.io.Printer._
import org.hammerlab.iterator.GroupWithIterator._
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.parallel.{ spark, threads }
import org.hammerlab.spark.Conf
import org.hammerlab.stats.Stats
import org.hammerlab.timing.Timer.time
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit }

import scala.collection.JavaConverters._

case class Args(@O("n") numThreads: Option[Int],
                @O("d") seqdoopOnly: Boolean = false,
                @O("c") compareSplits: Boolean = false,
                @O("r") printReadPartitionStats: Boolean = false,
                @O("g") gsBufferStr: Option[String] = None,
                @O("l") splitsPrintLimit: Option[Int] = None,
                @O("m") maxSplitSizeStr: Option[String] = None,
                @O("p") propertiesFiles: String = "",
                @O("s") sampleSize: Option[Int],
                @O("o") outFile: Option[String] = None) {
  def maxSplitSize(implicit conf: Configuration) =
    MaxSplitSize(
      maxSplitSizeStr
        .map(
          Size(_).bytes
        )
    )

  def parallelizer(implicit sc: SparkContext) =
    numThreads match {
      case Some(numWorkers) ⇒
        threads.Config(numWorkers)
      case _ ⇒
        spark.Config()
    }

  def gsBuffer: Option[Size]= gsBufferStr.map(Size(_))
}

object Main
  extends CaseApp[Args]
    with Logging {

  def getReads(args: Args, path: Path)(
      implicit
      sc: SparkContext,
      conf: Configuration
  ): BAMRecordRDD = {

    implicit val config =
      Config(
        parallelizer = args.parallelizer,
        maxSplitSize = args.maxSplitSize
      )

    time("get splits") {
      sc.loadBam(path)
    }
  }

  def getSeqdoopSplits(args: Args, path: Path)(implicit conf: Configuration): Seq[Split] = {
    val ifmt = new BAMInputFormat

    val job = Job.getInstance(conf)
    val jobConf = job.getConfiguration

    val jobID = new JobID("get-splits", 1)
    val jc = new JobContextImpl(jobConf, jobID)

    setInputPaths(job, path)

    time("get splits") {
      ifmt
        .getSplits(jc)
        .asScala
        .map(
          _.asInstanceOf[FileVirtualSplit]: Split
        )
    }
  }

  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {
    if (remainingArgs.remainingArgs.size != 1) {
      throw new IllegalArgumentException(
        s"Exactly one argument (a BAM file path) is required"
      )
    }

    val path = Path(remainingArgs.remainingArgs.head)

    val sparkConf = Conf(args.propertiesFiles)
    implicit val sc = new SparkContext(sparkConf)
    implicit val conf: Configuration = sc

    args
      .gsBuffer
      .foreach(
        conf.setLong(
          "fs.gs.io.buffersize",
          _
        )
      )

    conf.setLong(
      SPLIT_MAXSIZE,
      args.maxSplitSize
    )

    implicit val printer = Printer(args.outFile)
    implicit val sampleSize = SampleSize(args.sampleSize)

    def printSplits(splits: Seq[Split]): Unit = {
      val splitSizeStats = Stats(splits.map(_.length.toInt))
      print(
        "Split-size distribution:",
        splitSizeStats,
        ""
      )
      printList(
        splits,
        s"${splits.length} org.seqdoop.hadoop_bam splits:",
        n ⇒ s"First $n org.seqdoop.hadoop_bam splits:"
      )
    }

    (args.seqdoopOnly, args.compareSplits) match {
      case (false, true) ⇒
        val BAMRecordRDD(ourSplits, _) = getReads(args, path)
        val theirSplits = getSeqdoopSplits(args, path)

        implicit def toStart(split: Split): Pos = split.start

        val diffs =
          ourSplits
            .iterator
            .groupWith[Split, Pos](
              theirSplits.iterator
            )
            .map {
              case (ourSplit, theirSplits) ⇒
                ourSplit → theirSplits.toVector
            }
            .filter(_._2.length != 1)
            .toVector

        if (diffs.nonEmpty) {
          print("Differing splits:")
          for {
            (ourSplit, theirSplits) ← diffs
          } {
            if (theirSplits.isEmpty)
              print(s"$ourSplit: ∅")
            else
              print(
                s"$ourSplit:",
                theirSplits.mkString("\t\t", "\n\t\t", "")
              )
          }
        } else
          print("All splits matched!")

      case (true, false) ⇒
        val theirSplits = getSeqdoopSplits(args, path)
        printSplits(theirSplits)
      case (false, false) ⇒
        val BAMRecordRDD(ourSplits, reads) = getReads(args, path)
        printSplits(ourSplits)
        if (args.printReadPartitionStats) {
          val partitionSizes = reads.partitionSizes
          val partitionSizeStats = Stats(partitionSizes)
          print(
            "Partition count stats:",
            partitionSizeStats
          )
        }
      case (true, true) ⇒
        throw new IllegalArgumentException(
          s"Provide only one of {'-c', '-d'}"
        )
    }
  }
}
