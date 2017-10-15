package org.hammerlab.bam.spark

import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.AsNewHadoopPartition
import org.hammerlab.args.SplitSize
import org.hammerlab.cli.app.spark.PathApp
import org.hammerlab.collection.canBuildVector
import org.hammerlab.paths.Path
import org.hammerlab.timing.Timer
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit, SAMRecordWritable }

trait LoadReads {
  self: PathApp[_] with Timer ⇒

  def sparkBamLoad(implicit
                   args: SplitSize.Args,
                   path: Path
                  ): BAMRecordRDD =
    sc.loadSplitsAndReads(
      path,
      splitSize = args.maxSplitSize
    )

  def hadoopBamLoad(implicit
                    args: SplitSize.Args,
                    path: Path
                   ): BAMRecordRDD = {
    args.set

    val rdd =
      sc.newAPIHadoopFile(
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
