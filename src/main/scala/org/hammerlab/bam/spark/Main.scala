package org.hammerlab.bam.spark

import caseapp.{ ExtraName ⇒ O }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.{ SPLIT_MAXSIZE, setInputPaths }
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.hadoop.mapreduce.{ Job, JobID }
import org.hammerlab.app.{ SparkPathApp, SparkPathAppArgs }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bytes.Bytes
import org.hammerlab.hadoop
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.io.Printer._
import org.hammerlab.io.SampleSize
import org.hammerlab.iterator.GroupWithIterator._
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.paths.Path
import org.hammerlab.stats.Stats
import org.hammerlab.timing.Timer.time
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit }

import scala.collection.JavaConverters._

case class Args(@O("c") compare: Boolean = false,
                @O("g") gsBuffer: Option[Bytes] = None,
                @O("l") printLimit: SampleSize = SampleSize(None),
                @O("m") splitSize: Option[Bytes] = None,
                @O("o") out: Option[Path] = None,
                @O("p") printReadPartitionStats: Boolean = false,
                @O("s") seqdoop: Boolean = false,
                @O("t") threads: Option[Int]
               )
  extends SparkPathAppArgs {
  def parallelizer =
    threads match {
      case Some(numWorkers) ⇒
        Threads(numWorkers)
      case _ ⇒
        Spark()
    }
}

object Main
  extends SparkPathApp[Args] {

  def getReads(args: Args,
               path: Path): BAMRecordRDD =
    time("get splits") {
      sc.loadBam(
        path,
        parallelConfig = args.parallelizer,
        splitSize = MaxSplitSize(args.splitSize)
      )
    }

  def getSeqdoopSplits(args: Args, path: Path): Seq[Split] = {
    val ifmt = new BAMInputFormat

    val job = Job.getInstance(ctx)
    val jobConf = job.getConfiguration

    val jobID = new JobID("get-splits", 1)
    val jc = new JobContextImpl(jobConf, jobID)

    setInputPaths(job, hadoop.Path(path.uri))

    time("get splits") {
      ifmt
        .getSplits(jc)
        .asScala
        .map(
          _.asInstanceOf[FileVirtualSplit]: Split
        )
    }
  }

  override def run(args: Args): Unit = {
    args
      .gsBuffer
      .foreach(
        ctx.setLong(
          "fs.gs.io.buffersize",
          _
        )
      )

    args
      .splitSize
        .foreach(
          ctx.setLong(
            SPLIT_MAXSIZE,
            _
          )
        )

    def printSplits(splits: Seq[Split]): Unit = {
      val splitSizeStats = Stats(splits.map(_.length.toInt))
      echo(
        "Split-size distribution:",
        splitSizeStats,
        ""
      )
      print(
        splits,
        s"${splits.length} org.seqdoop.hadoop_bam splits:",
        n ⇒ s"First $n org.seqdoop.hadoop_bam splits:"
      )
    }

    (args.seqdoop, args.compare) match {
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
          echo("Differing splits:")
          for {
            (ourSplit, theirSplits) ← diffs
          } {
            if (theirSplits.isEmpty)
              echo(s"$ourSplit: ∅")
            else
              echo(
                s"$ourSplit:",
                theirSplits.mkString("\t\t", "\n\t\t", "")
              )
          }
        } else
          echo("All splits matched!")

      case (true, false) ⇒
        val theirSplits = getSeqdoopSplits(args, path)
        printSplits(theirSplits)
      case (false, false) ⇒
        val BAMRecordRDD(ourSplits, reads) = getReads(args, path)
        printSplits(ourSplits)
        if (args.printReadPartitionStats) {
          val partitionSizes = reads.partitionSizes
          val partitionSizeStats = Stats(partitionSizes)
          echo(
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
