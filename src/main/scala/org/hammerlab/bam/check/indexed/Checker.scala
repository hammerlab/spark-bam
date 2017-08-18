package org.hammerlab.bam.check.indexed

import caseapp.{ ValueDescription, ExtraName ⇒ O, HelpMessage ⇒ M }
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.args.ByteRanges
import org.hammerlab.bam.check
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
  case class Args(
    @O("r")
    @ValueDescription("path")
    @M("File with BAM-record-start positions, as output by index-records. If unset, the BAM path with a \".records\" extension appended is used")
    recordsPath: Option[Path]
  ) {
    def path(implicit bamPath: Path): Path =
      recordsPath
        .getOrElse(bamPath + ".records")
  }

  def apply(path: Path)(
      implicit
      sc: SparkContext,
      rangesBroadcast: Broadcast[Option[ByteRanges]]
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
            rangesBroadcast
              .value
              .forall(_.contains(blockPos))
        }
        .cache

    IndexedRecordPositions(
      reads,
      bounds(reads)
    )
  }
}
