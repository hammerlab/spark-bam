package org.hammerlab.bam.spark

import caseapp.core.{ ArgParser, ContextArgParser, ContextParser }
import caseapp.{ CaseApp, RemainingArgs, ExtraName ⇒ O }
import grizzled.slf4j.Logging
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.{ SPLIT_MAXSIZE, setInputPaths }
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.hadoop.mapreduce.{ Job, JobID }
import org.apache.spark.SparkContext
import org.hammerlab.SparkApp
import org.hammerlab.bam.spark.LoadBam._
import org.hammerlab.bgzf.Pos
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.hadoop.{ Configuration, Path }
import org.hammerlab.io.Printer._
import org.hammerlab.io.{ Printer, SampleSize, Size }
import org.hammerlab.iterator.GroupWithIterator._
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.parallel.{ spark, threads }
import org.hammerlab.spark.{ Conf, Context }
import org.hammerlab.stats.Stats
import org.hammerlab.timing.Timer.time
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit }

import scala.collection.JavaConverters._
import org.hammerlab.parallel.spark.PartitioningStrategy

case class Args(@O("n") numThreads: Option[Int],
                @O("d") seqdoopOnly: Boolean = false,
                @O("c") compareSplits: Boolean = false,
                @O("r") printReadPartitionStats: Boolean = false,
                @O("g") gsBuffer: Option[Size] = None,
                @O("l") splitsPrintLimit: Option[Int] = None,
                @O("m") maxSplitSize: Option[MaxSplitSize] = None,
                @O("p") propertiesFiles: String = "",
                @O("s") sampleSize: Option[Int],
                @O("o") outFile: Option[Path] = None) {
  def parallelizer(implicit sc: Context) =
    numThreads match {
      case Some(numWorkers) ⇒
        threads.Config(numWorkers)
      case _ ⇒
        spark.Config()(sc, implicitly[PartitioningStrategy])
    }
}

object Args {

  ContextArgParser.apply[Configuration, Int]
  ContextArgParser.apply[Configuration, Boolean]
  ContextArgParser.apply[Configuration, Size]
  ContextArgParser.apply[Configuration, MaxSplitSize]
  ContextArgParser.apply[Configuration, Path]

  ContextArgParser.apply[Configuration, Option[Int]]
  ContextArgParser.apply[Configuration, Option[Size]]
  ContextArgParser.apply[Configuration, Option[MaxSplitSize]]
  ContextArgParser.apply[Configuration, Option[Path]]

  val contextParser = implicitly[ContextParser[Configuration, Args]]
}

object Main
  extends SparkApp[Args]
    with Logging {

  def getReads(args: Args, path: Path)(
      implicit
      sc: Context
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

  def getSeqdoopSplits(args: Args, path: Path): Seq[Split] = {
    val ifmt = new BAMInputFormat

    val job = Job.getInstance(sc)
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

  override def run(args: Args, remainingArgs: Seq[String]): Unit = {
    if (remainingArgs.size != 1) {
      throw new IllegalArgumentException(
        s"Exactly one argument (a BAM file path) is required"
      )
    }

    val path = Path(remainingArgs.head)

    args
      .gsBuffer
      .foreach(
        sc.setLong(
          "fs.gs.io.buffersize",
          _
        )
      )

    sc.setLong(
      SPLIT_MAXSIZE,
      (args.maxSplitSize: MaxSplitSize)
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
