package org.hammerlab.bam.spark.load

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input
import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

case class FileSplitPartition(index: Int,
                              start: Long,
                              end: Long,
                              locations: Array[String])
  extends Partition

class SplitRDD private(@transient override val getPartitions: Array[Partition])(implicit sc: SparkContext)
  extends RDD[(Long, Long)](sc, Nil) {
  override def compute(split: Partition, context: TaskContext) =
    Iterator(
      split.asInstanceOf[FileSplitPartition]
    )
    .map(
      fs ⇒
        fs.start → fs.end
    )

  override protected def getPreferredLocations(split: Partition) =
    split
      .asInstanceOf[FileSplitPartition]
      .locations
}

object SplitRDD {
  def apply(splits: java.util.List[InputSplit])(implicit sc: SparkContext): SplitRDD =
    new SplitRDD(
      splits
        .iterator()
        .asScala
        .map(_.asInstanceOf[input.FileSplit])
        .zipWithIndex
        .map {
          case (fs, idx) ⇒
            FileSplitPartition(
              idx,
              fs.getStart,
              fs.getStart + fs.getLength,
              fs.getLocations
            )
        }
        .toArray
    )
}

