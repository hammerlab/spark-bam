package org.hammerlab.bam.check.eager

import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.False
import org.hammerlab.bgzf.Pos

case class Result(numPositions: Long,
                  positionResults: RDD[(Pos, PosResult)],
                  numFalseCalls: Long,
                  falseCalls: RDD[(Pos, False)],
                  numReadStarts: Long,
                  readStarts: RDD[Pos])
  extends check.Result[PosResult]
