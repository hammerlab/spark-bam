package org.hammerlab.bam.spark

import caseapp.{ ExtraName ⇒ O }
import cats.implicits.catsStdShowForInt
import cats.syntax.all._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE
import org.apache.spark.rdd.AsNewHadoopPartition
import org.hammerlab.app.{ SparkPathApp, SparkPathAppArgs }
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.spark.Main.time
import org.hammerlab.bgzf.Pos
import org.hammerlab.bytes.Bytes
import org.hammerlab.collection.canBuildVector
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.io.Printer._
import org.hammerlab.io.SampleSize
import org.hammerlab.iterator.sorted.OrZipIterator._
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.paths.Path
import org.hammerlab.spark.Context
import org.hammerlab.stats.Stats
import org.hammerlab.timing.Timer
import org.hammerlab.types._
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit, SAMRecordWritable }

trait SplitsArgs {
  def splitSize: Option[Bytes]
}

case class Args(@O("c") compare: Boolean = false,
                @O("g") gsBuffer: Option[Bytes] = None,
                @O("l") printLimit: SampleSize = SampleSize(None),
                @O("m") splitSize: Option[Bytes] = None,
                @O("o") out: Option[Path] = None,
                @O("p") printReadPartitionStats: Boolean = false,
                @O("s") seqdoop: Boolean = false
               )
  extends SparkPathAppArgs
    with SplitsArgs {
}

trait CanCompareSplits {
  implicit def ctx: Context

  def sparkBamLoad(args: SplitsArgs,
                   path: Path): BAMRecordRDD =
    time("Get spark-bam splits") {
      ctx.loadSplitsAndReads(
        path,
        splitSize = MaxSplitSize(args.splitSize)
      )
    }

  def hadoopBamLoad(args: SplitsArgs,
                    path: Path): BAMRecordRDD =
    time("Get hadoop-bam splits") {

      args
        .splitSize
        .foreach(
          ctx
            .setLong(
              SPLIT_MAXSIZE,
              _
            )
        )

      val rdd =
        ctx.newAPIHadoopFile(
          path.toString(),
          classOf[BAMInputFormat],
          classOf[LongWritable],
          classOf[SAMRecordWritable]
        )

      val reads =
        rdd
          .values
          .map(_.get())

      val partitions =
        rdd
          .partitions
          .map(AsNewHadoopPartition(_))
          .map[Split, Vector[Split]](
            _
              .serializableHadoopSplit
              .value
              .asInstanceOf[FileVirtualSplit]: Split
          )

      BAMRecordRDD(partitions, reads)
    }
}

object Main
  extends SparkPathApp[Args](classOf[Registrar])
    with Timer
    with CanCompareSplits {

  override def run(args: Args): Unit = {
    args
      .gsBuffer
      .foreach(
        ctx.setLong(
          "fs.gs.io.buffersize",
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
        val our = sparkBamLoad(args, path)

        info("Computing hadoop-bam splits")
        val their = hadoopBamLoad(args, path)

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
                ours.show
              case Right(theirs) ⇒
                show"\t$theirs"
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
        val BAMRecordRDD(splits, reads) = hadoopBamLoad(args, path)
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
        val BAMRecordRDD(splits, reads) = sparkBamLoad(args, path)
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
