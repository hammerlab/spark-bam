package org.hammerlab.bam.check.indexed

import caseapp.{ ValueDescription, ExtraName ⇒ O, HelpMessage ⇒ M }
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.hammerlab.args.ByteRanges
import org.hammerlab.bam.check.Blocks
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.Metadata
import org.hammerlab.kryo.Registrar
import org.hammerlab.magic.rdd.partitions.RangePartitionRDD._
import org.hammerlab.magic.rdd.partitions.SortedRDD
import org.hammerlab.magic.rdd.partitions.SortedRDD.{ Bounds, bounds }
import org.hammerlab.paths.Path

import scala.collection.immutable.SortedSet
import scala.reflect.ClassTag

case class IndexedRecordPositions(rdd: RDD[Pos],
                                  bounds: Bounds[Pos])(
    implicit o: Ordering[Pos]
)
  extends SortedRDD[Pos] {

  override implicit def ord: Ordering[Pos] = o

  def toSets(newBounds: Bounds[Pos]): RDD[SortedSet[Pos]] =
    this
      .sortedRepartition(newBounds)
      .rdd
      .mapPartitions(it ⇒ Iterator(SortedSet(it.toSeq: _*)(o)))

}

object IndexedRecordPositions
  extends Registrar {

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

  def apply[U: ClassTag]()(
      implicit
      path: Path,
      sc: SparkContext,
      rangesBroadcast: Broadcast[Option[ByteRanges]],
      blockArgs: Blocks.Args,
      recordArgs: IndexedRecordPositions.Args
  ): (RDD[Metadata], RDD[SortedSet[Pos]]) = {

    val Blocks(blocks, bounds) = Blocks()

    val posBounds =
      bounds
        .copy(
          partitions =
            bounds
            .partitions
            .map {
              _.map {
                case (start, endOpt) ⇒
                  (
                    Pos(start, 0),
                    endOpt.map(Pos(_, 0))
                  )
              }
            }
        )

    val indexedRecords = IndexedRecordPositions(recordArgs.path)

    val repartitionedRecords = indexedRecords.toSets(posBounds)

    (
      blocks,
      repartitionedRecords
    )
  }

  register(Blocks)
}
