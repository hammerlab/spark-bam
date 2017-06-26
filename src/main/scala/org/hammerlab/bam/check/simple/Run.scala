package org.hammerlab.bam.check.simple

import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.False
import org.hammerlab.bgzf.Pos
import org.hammerlab.io.SampleSize

trait Run
  extends check.Run[Boolean, PosResult, Result] {

  override def makePosResult: check.MakePosResult[Boolean, PosResult] = MakePosResult

  override def makeResult(numCalls: Long,
                          results: RDD[(Pos, PosResult)],
                          numFalseCalls: Long,
                          falseCalls: RDD[(Pos, False)],
                          numCalledReadStarts: Long,
                          calledReadStarts: RDD[Pos])(implicit sampleSize: SampleSize): Result =
    Result(
      numCalls,
      results,
      numFalseCalls,
      falseCalls,
      numCalledReadStarts,
      calledReadStarts
    )
}

