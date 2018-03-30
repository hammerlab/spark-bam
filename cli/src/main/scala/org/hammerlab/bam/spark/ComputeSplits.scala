package org.hammerlab.bam.spark

import caseapp.{ AppName, ProgName, HelpMessage ⇒ M, Name ⇒ O, Recurse ⇒ R }
import hammerlab.bytes._
import hammerlab.iterator._
import hammerlab.lines.limit._
import hammerlab.or._
import hammerlab.show._
import magic_rdds.partitions._
import org.hammerlab.args.SplitSize
import org.hammerlab.bgzf.Pos
import hammerlab.cli._
import hammerlab.cli.spark.PathApp
import org.hammerlab.stats.Stats
import org.hammerlab.timing.Timer

object ComputeSplits extends Cmd {

  @AppName("Compute and print BAM-splits using spark-bam and/or hadoop-bam; if both, compare the two as well")
  @ProgName("… org.hammerlab.bam.spark.Main")
  case class Opts(
      @R printLimit: PrintLimitArgs,
      @R splitSizeArgs: SplitSize.Args,

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

  val main = Main(
    args ⇒
      new PathApp(args, load.Registrar)
        with Timer
        with LoadReads {

        args
          .gsBuffer
          .foreach(
            conf.setLong(
              "fs.gs.io.buffersize",
              _
            )
          )

        def printSplits(splits: Seq[Split]): Unit = {
          val splitSizeStats = Stats(splits.map(_.length.toInt))
          echo(
            "Split-size distribution:",
            splitSizeStats,
            "",
            Limited(
              splits,
              s"${splits.length} splits:",
              s"First $limit of ${splits.length} splits:"
            )
          )
        }

        implicit val splitSizeArgs = args.splitSizeArgs

        (args.sparkBam, args.hadoopBam) match {
          case (false, true) ⇒
            val (hadoopBamMS, BAMRecordRDD(splits, reads)) = time { hadoopBamLoad }
            echo(
              s"Get hadoop-bam splits: ${hadoopBamMS}ms",
              ""
            )
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
            val (sparkBamMS, BAMRecordRDD(splits, reads)) = time { sparkBamLoad }
            echo(
              s"Get spark-bam splits: ${sparkBamMS}ms",
              ""
            )
            printSplits(splits)
            if (args.printReadPartitionStats)
              echo(
                "Partition count stats:",
                Stats(reads.partitionSizes)
              )
          case _ ⇒
            info("Computing spark-bam splits")
            val (sparkBamMS, our) = time { sparkBamLoad }
            echo(s"Get spark-bam splits: ${sparkBamMS}ms")

            info("Computing hadoop-bam splits")
            val (hadoopBamMS, their) = time { hadoopBamLoad }
            echo(s"Get hadoop-bam splits: ${hadoopBamMS}ms")

            echo("")

            implicit def toStart(split: Split): Pos = split.start

            val diffs =
              our
                .splits
                .orMerge[Split, Pos](their.splits)
                .flatMap {
                  case Both(_, _) ⇒ None
                  case L(ours) ⇒ Some(Left(ours))
                  case R(theirs) ⇒ Some(Right(theirs))
                }
                .toVector

            if (diffs.nonEmpty)
              echo(
                Limited(
                  diffs
                    .map {
                      case Left(ours) ⇒ ours.show
                      case Right(theirs) ⇒ show"\t$theirs"
                    },
                  s"${diffs.length} splits differ (totals: ${our.splits.size}, ${their.splits.length}):",
                  s"First $limit of ${diffs.length} splits that differ (totals: ${our.splits.size}, ${their.splits.length}):"
                )
              )
            else {
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
  )
}
