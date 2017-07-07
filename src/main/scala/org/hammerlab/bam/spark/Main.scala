package org.hammerlab.bam.spark

import caseapp.{ ExtraName ⇒ O }
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE
import org.apache.spark.rdd.HadoopPartition
import org.hammerlab.app.{ SparkPathApp, SparkPathAppArgs }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bytes.Bytes
import org.hammerlab.collection.canBuildVector
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.io.Printer._
import org.hammerlab.io.SampleSize
import org.hammerlab.iterator.sorted.OrZipIterator._
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.paths.Path
import org.hammerlab.stats.Stats
import org.hammerlab.timing.Timer.time
import org.hammerlab.types._
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit, SAMRecordWritable }

case class Args(@O("c") compare: Boolean = false,
                @O("g") gsBuffer: Option[Bytes] = None,
                @O("l") printLimit: SampleSize = SampleSize(None),
                @O("m") splitSize: Option[Bytes] = None,
                @O("o") out: Option[Path] = None,
                @O("p") printReadPartitionStats: Boolean = false,
                @O("s") seqdoop: Boolean = false,
                @O("t") threads: Int = 0
               )
  extends SparkPathAppArgs {
  def parallelizer =
    if (threads == 0)
      Spark()
    else
      Threads(threads)
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

  def getSeqdoopSplits(args: Args, path: Path): BAMRecordRDD = {

    val rdd =
      time("get splits") {
        sc.newAPIHadoopFile(
          path.toString(),
          classOf[BAMInputFormat],
          classOf[LongWritable],
          classOf[SAMRecordWritable]
        )
      }

    val reads =
      rdd
        .values
        .map(_.get())

    val partitions =
      rdd
        .partitions
        .map(HadoopPartition(_))
        .map[Split, Vector[Split]](
          _
            .serializableHadoopSplit
            .value
            .asInstanceOf[FileVirtualSplit]: Split
        )

    BAMRecordRDD(partitions, reads)
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
        s"${splits.length} splits:",
        n ⇒ s"First $n of ${splits.length} splits:"
      )
    }

    (args.seqdoop, args.compare) match {
      case (false, true) ⇒
        info("Computing spark-bam splits")
        val our = getReads(args, path)

        info("Computing hadoop-bam splits")
        val their = getSeqdoopSplits(args, path)

        implicit def toStart(split: Split): Pos = split.start

        val diffs =
          our
            .splits
            .sortedOrZip[Split, Pos](their.splits)
            .flatMap {
              case Both(_, _) ⇒ None
              case LO(ours) ⇒ Some(Left(ours))
              case RO(theirs) ⇒ Some(Right(theirs))
            }
            .toVector

        if (diffs.nonEmpty) {
          print(
            diffs
              .map {
              case Left(ours) ⇒
                ours.toString
              case Right(theirs) ⇒
                s"\t$theirs"
            },
            s"${diffs.length} splits differ (totals: ${our.splits.size}, ${their.splits.length}):",
            n ⇒ s"First $n of ${diffs.length} splits that differ (totals: ${our.splits.size}, ${their.splits.length}):"
          )
        } else {
          echo(
            "All splits matched!",
            ""
          )
          printSplits(our.splits)
          if (args.printReadPartitionStats) {
            val partitionSizes = our.reads.partitionSizes
            val partitionSizeStats = Stats(partitionSizes)
            echo(
              "Partition count stats:",
              partitionSizeStats
            )
          }
        }

      case (true, false) ⇒
        val BAMRecordRDD(splits, reads) = getSeqdoopSplits(args, path)
        printSplits(splits)
        if (args.printReadPartitionStats) {
          val partitionSizes = reads.partitionSizes
          val partitionSizeStats = Stats(partitionSizes)
          echo(
            "Partition count stats:",
            partitionSizeStats
          )
        }
      case (false, false) ⇒
        val BAMRecordRDD(splits, reads) = getReads(args, path)
        printSplits(splits)
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
