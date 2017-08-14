package org.hammerlab.bam.check.indexed

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.Whitelist
import org.hammerlab.bgzf.Pos
import org.hammerlab.magic.rdd.partitions.RangePartitionRDD._
import org.hammerlab.magic.rdd.partitions.SortedRDD
import org.hammerlab.magic.rdd.partitions.SortedRDD.{ Bounds, bounds }
import org.hammerlab.paths.Path

case class Checker(readPositions: Set[Pos])
  extends check.Checker[Boolean] {
  override def apply(pos: Pos): Boolean = readPositions(pos)
}

case class IndexedRecordPositions(rdd: RDD[Pos],
                                  bounds: Bounds[Pos])(
    implicit o: Ordering[Pos]
)
  extends SortedRDD[Pos] {

  override implicit def ord: Ordering[Pos] = o

  def toSets(newBounds: Bounds[Pos]): RDD[Set[Pos]] =
    this
      .sortedRepartition(newBounds)
      .rdd
      .mapPartitions(it ⇒ Iterator(it.toSet))

}

object IndexedRecordPositions {
  trait Args {
    def records: Option[Path]
    def recordsPath(implicit path: Path): Path =
      records
        .getOrElse(path + ".records")
  }

  def apply(path: Path,
            whitelistBroadcast: Broadcast[Option[Whitelist]])(
      implicit sc: SparkContext
  ): IndexedRecordPositions = {
    val reads =
      sc
        .textFile(path.toString)
        .map(
          line ⇒
            line.split(",") match {
              case Array(a, b) ⇒
                Pos(a.toLong, b.toInt)
              case _ ⇒
                throw new IllegalArgumentException(
                  s"Bad record-pos line: $line"
                )
            }
        )
        .filter {
          case Pos(blockPos, _) ⇒
            whitelistBroadcast
            .value
            .forall(_.blocks(blockPos))
        }
        .cache

    IndexedRecordPositions(
      reads,
      bounds(reads)
    )
  }
}
