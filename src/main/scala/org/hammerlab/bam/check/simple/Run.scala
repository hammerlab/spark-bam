package org.hammerlab.bam.check.simple

import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.{ Args, False }
import org.hammerlab.bgzf.Pos
import org.hammerlab.magic.rdd.partitions.OrderedRepartitionRDD._
import org.hammerlab.magic.rdd.size._
import org.hammerlab.math.ceil
import org.hammerlab.paths.Path
import org.hammerlab.spark.Context

trait Run
  extends check.Run[Boolean, PosResult] {
  override def makePosResult: check.MakePosResult[Boolean, PosResult] =
    MakePosResult

  def apply(args: Args)(implicit sc: Context, path: Path): Result = {
    val (calls, blocks) = getCalls(args)

    val numResultPartitions =
      ceil(
        blocks.uncompressedSize,
        args.resultsPerPartition
      )
      .toInt

    val readPositions =
      calls
        .filter(_._2)
        .keys
        .orderedRepartition(numResultPartitions)

    val trueReadPositions =
      getTrueReadPositions(
        args,
        blocks
      )

    implicit val sampleSize = args.printLimit

    val falseCalls: RDD[(Pos, False)] =
      readPositions
        .map(_ → null)
        .fullOuterJoin(
          trueReadPositions.map(
            _ → null
          )
        )
        .mapValues {
          case (readPos, trueReadPos) ⇒
            (
              readPos.isDefined,
              trueReadPos.isDefined
            )
        }
        .flatMap {
          case (pos, (calledRead, isRead)) ⇒
            (calledRead, isRead) match {
              case ( true,  true) ⇒ None
              case ( true, false) ⇒ Some(pos → (FalsePositive: False))
              case (false,  true) ⇒ Some(pos → (FalseNegative: False))
              case (false, false) ⇒
                throw new IllegalStateException(
                  s"Position $pos emitted from join, but no read or called read there"
                )
            }
        }
        .sortByKey()
        .cache

    Result(
      blocks.uncompressedSize,
      falseCalls.size,
      falseCalls,
      trueReadPositions.size
    )
  }
}

