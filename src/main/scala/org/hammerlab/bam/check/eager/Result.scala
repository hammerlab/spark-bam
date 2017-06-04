package org.hammerlab.bam.check.eager

import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.False
import org.hammerlab.bgzf.Pos

case class Result(numCalls: Long,
                  results: RDD[(Pos, PosResult)],
                  numFalseCalls: Long,
                  falseCalls: RDD[(Pos, False)])
  extends check.Result[PosResult]
