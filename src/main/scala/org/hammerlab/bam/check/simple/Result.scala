package org.hammerlab.bam.check.simple

import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.False
import org.hammerlab.bgzf.Pos
import org.hammerlab.io.SampleSize

case class Result(numPositions: Long,
                  positionResults: RDD[(Pos, PosResult)],
                  numFalseCalls: Long,
                  falseCalls: RDD[(Pos, False)],
                  numCalledReadStarts: Long,
                  calledReadStarts: RDD[Pos])(implicit sampleSize: SampleSize)
  extends check.Result[PosResult]
