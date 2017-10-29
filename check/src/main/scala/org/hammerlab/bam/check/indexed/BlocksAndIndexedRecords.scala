package org.hammerlab.bam.check.indexed

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.args.ByteRanges
import org.hammerlab.bam.check.Blocks
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.Metadata
import org.hammerlab.kryo.Registrar
import org.hammerlab.paths.Path

import scala.collection.immutable.SortedSet
import scala.reflect.ClassTag

case class BlocksAndIndexedRecords(blocks: RDD[Metadata],
                                   records: RDD[SortedSet[Pos]])

object BlocksAndIndexedRecords
  extends Registrar {

  def apply[U: ClassTag]()(
      implicit
      path: Path,
      sc: SparkContext,
      rangesBroadcast: Broadcast[Option[ByteRanges]],
      blockArgs: Blocks.Args,
      recordArgs: IndexedRecordPositions.Args
  ): BlocksAndIndexedRecords = {

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

    BlocksAndIndexedRecords(
      blocks,
      repartitionedRecords
    )
  }

  register(
    Blocks,
    IndexedRecordPositions
  )
}
