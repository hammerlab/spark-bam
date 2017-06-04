package org.hammerlab.bam.check.eager

import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.{ False, MakePosResult }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableByteStream
import org.hammerlab.genomics.reference.NumLoci

object Run
  extends check.Run[Boolean, PosResult] {

  override def makeChecker: (SeekableByteStream, Map[Int, NumLoci]) ⇒ Checker =
    Checker.apply

  override def makePosResult: MakePosResult[Boolean, PosResult] = MakePosResult

  override def makeResult(numCalls: Long,
                          results: RDD[(Pos, PosResult)],
                          numFalseCalls: Long,
                          falseCalls: RDD[(Pos, False)]): Result =
    Result(
      numCalls,
      results,
      numFalseCalls,
      falseCalls
    )
}

