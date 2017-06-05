package org.hammerlab.bam.check

import org.apache.spark.rdd.RDD
import org.hammerlab.bgzf.Pos

trait Result[PosResult] {

  def numPositions: Long
  def positionResults: RDD[(Pos, PosResult)]

  def numFalseCalls: Long
  def falseCalls: RDD[(Pos, False)]

  def numReadStarts: Long
  def readStarts: RDD[Pos]

  var falseCallsSampleSize = 100

  lazy val falseCallsSample =
    if (numFalseCalls > falseCallsSampleSize)
      falseCalls.take(falseCallsSampleSize)
    else if (numFalseCalls > 0)
      falseCalls.collect()
    else
      Array()

  lazy val falseCallsHist =
    falseCalls
      .values
      .map(_ → 1L)
      .reduceByKey(_ + _)
      .map(_.swap)
      .sortByKey(ascending = false)

  var falseCallsHistSampleSize = 100

  lazy val falseCallsHistSample =
    if (numFalseCalls > falseCallsHistSampleSize)
      falseCallsHist.take(falseCallsHistSampleSize)
    else if (numFalseCalls > 0)
      falseCallsHist.collect()
    else
      Array()
}

object Result {
  def unapply[PosResult](result: Result[PosResult]): Option[(Long, Long, Long)] =
    Some(
      result.numPositions,
      result.numFalseCalls,
      result.numReadStarts
    )

  def sampleString(sampledLines: Seq[String], total: Long): String =
    sampledLines
      .mkString(
        "\t",
        "\n\t",
        if (sampledLines.size < total)
          "\n\t…"
        else
          ""
    )
}
