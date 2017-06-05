package org.hammerlab.bam.check.eager

import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.False
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableByteStream
import org.hammerlab.genomics.reference.NumLoci

object Run
  extends check.Run[Boolean, PosResult] {

  override def makeChecker: (SeekableByteStream, Map[Int, NumLoci]) ⇒ Checker =
    Checker.apply

  override def makePosResult: check.MakePosResult[Boolean, PosResult] = MakePosResult

  override def makeResult(numCalls: Long,
                          results: RDD[(Pos, PosResult)],
                          numFalseCalls: Long,
                          falseCalls: RDD[(Pos, False)],
                          numReadStarts: Long,
                          readStarts: RDD[Pos]): Result =
    Result(
      numCalls,
      results,
      numFalseCalls,
      falseCalls,
      numReadStarts,
      readStarts
    )
}

