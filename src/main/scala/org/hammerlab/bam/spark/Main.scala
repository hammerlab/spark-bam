package org.hammerlab.bam.spark

import caseapp.{ AppName, ProgName, Recurse, ExtraName ⇒ O, HelpMessage ⇒ M }
import cats.implicits.catsStdShowForInt
import cats.syntax.all._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE
import org.apache.spark.rdd.AsNewHadoopPartition
import org.hammerlab.app.{ SparkPathApp, SparkPathAppArgs }
import org.hammerlab.args.{ OutputArgs, SplitSize }
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.spark.Main.time
import org.hammerlab.bgzf.Pos
import org.hammerlab.bytes.Bytes
import org.hammerlab.collection.canBuildVector
import org.hammerlab.io.Printer._
import org.hammerlab.iterator.sorted.OrZipIterator._
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.paths.Path
import org.hammerlab.spark.Context
import org.hammerlab.stats.Stats
import org.hammerlab.timing.Timer
import org.hammerlab.types._
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit, SAMRecordWritable }

@AppName("Compute and print BAM-splits using spark-bam and/or hadoop-bam; if both, compare the two as well")
@ProgName("… org.hammerlab.bam.spark.Main")
case class Args(
    @Recurse output: OutputArgs,
    @Recurse splitSizeArgs: SplitSize.Args,

    @O("g")
    @M("Set the buffer size (fs.gs.io.buffersize) used by the GCS-HDFS connector")
    gsBuffer: Option[Bytes] = None,

    @O("p")
    @M("Print extra statistics, about the distributions of computed split sizes and number of reads per partition")
    printReadPartitionStats: Boolean = false,

    @O("s")
    @M("Run the spark-bam checker; if both or neither of -s and -u are set, then they are both run, and the results compared. If only one is set, its computed splits are printed")
    sparkBam: Boolean = false,

    @O("upstream") @O("u")
    @M("Run the hadoop-bam checker; if both or neither of -s and -u are set, then they are both run, and the results compared. If only one is set, its computed splits are printed")
    hadoopBam: Boolean = false
)
  extends SparkPathAppArgs

trait CanCompareSplits {
  implicit def ctx: Context

  def sparkBamLoad(
      implicit
      args: SplitSize.Args,
      path: Path
  ): BAMRecordRDD =
    time("Get spark-bam splits") {
      ctx.loadSplitsAndReads(
        path,
        splitSize = args.maxSplitSize
      )
    }

  def hadoopBamLoad(
      implicit
      args: SplitSize.Args,
      path: Path
  ): BAMRecordRDD =
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

    implicit val splitSizeArgs = args.splitSizeArgs

    (args.sparkBam, args.hadoopBam) match {
      case (false, true) ⇒
        val BAMRecordRDD(splits, reads) = hadoopBamLoad
        printSplits(splits)
        if (args.printReadPartitionStats) {
          val partitionSizes = reads.partitionSizes
          val partitionSizeStats = Stats(partitionSizes)
          echo(
            "Partition count stats:",
            partitionSizeStats
          )
        }
      case (true, false) ⇒
        val BAMRecordRDD(splits, reads) = sparkBamLoad
        printSplits(splits)
        if (args.printReadPartitionStats)
          echo(
            "Partition count stats:",
            Stats(reads.partitionSizes)
          )
      case _ ⇒
        info("Computing spark-bam splits")
        val our = sparkBamLoad

        info("Computing hadoop-bam splits")
        val their = hadoopBamLoad

        implicit def toStart(split: Split): Pos = split.start

        val diffs =
          our
            .splits
            .sortedOrZip[Split, Pos](their.splits)
            .flatMap {
              case Both(_, _) ⇒ None
              case L(ours) ⇒ Some(Left(ours))
              case R(theirs) ⇒ Some(Right(theirs))
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
    }
  }
}
