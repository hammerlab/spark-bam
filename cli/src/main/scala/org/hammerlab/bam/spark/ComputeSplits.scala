package org.hammerlab.bam.spark

import caseapp.{ AppName, ProgName, Recurse, ExtraName ⇒ O, HelpMessage ⇒ M }
import cats.implicits.catsStdShowForInt
import cats.syntax.all._
import org.hammerlab.args.SplitSize
import org.hammerlab.bgzf.Pos
import org.hammerlab.bytes.Bytes
import org.hammerlab.cli.app
import org.hammerlab.cli.app.{ Args, Cmd }
import org.hammerlab.cli.app.spark.PathApp
import org.hammerlab.cli.args.PrintLimitArgs
import org.hammerlab.io.Printer._
import org.hammerlab.iterator.sorted.OrZipIterator._
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.stats.Stats
import org.hammerlab.timing.Timer
import org.hammerlab.types._

object ComputeSplits extends Cmd {

  @AppName("Compute and print BAM-splits using spark-bam and/or hadoop-bam; if both, compare the two as well")
  @ProgName("… org.hammerlab.bam.spark.Main")
  case class Opts(
      @Recurse printLimit: PrintLimitArgs,
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

//  object Main extends app.Main(App)

//  val fn: () ⇒ load.Registrar = load.Registrar
  
  val main = Main(
    args ⇒ new PathApp(args, load.Registrar)
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
  )
}
