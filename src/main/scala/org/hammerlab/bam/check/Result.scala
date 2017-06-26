package org.hammerlab.bam.check

import org.apache.spark.rdd.RDD
import org.hammerlab.bgzf.Pos
import org.hammerlab.io.Printer._
import org.hammerlab.io.{ Printer, SampleSize }
import org.hammerlab.magic.rdd.size._
import org.hammerlab.spark.SampleRDD.sample

abstract class Result[PosResult](implicit sampleSize: SampleSize) {

  def numPositions: Long
  def positionResults: RDD[(Pos, PosResult)]

  def numFalseCalls: Long
  def falseCalls: RDD[(Pos, False)]

  def numCalledReadStarts: Long
  def calledReadStarts: RDD[Pos]

  lazy val falseCallsSample = sample(falseCalls, numFalseCalls)

  lazy val falseCallsHist =
    falseCalls
      .values
      .map(_ → 1L)
      .reduceByKey(_ + _)
      .map(_.swap)
      .sortByKey(ascending = false)

  lazy val falseCallsHistSize = falseCallsHist.size
  lazy val falseCallsHistSample = sample(falseCallsHist, falseCallsHistSize)

  def prettyPrint(implicit printer: Printer): Unit =
    numFalseCalls match {
      case 0 ⇒
        print(
          s"$numPositions calls ($numCalledReadStarts reads), no errors!"
        )
      case _ ⇒
        print(
          s"$numPositions calls ($numCalledReadStarts reads), $numFalseCalls errors"
        )

        printSamples(
          falseCallsHistSample,
          falseCallsHistSize,
          "False-call histogram:",
          n ⇒ s"First $n false-call histogram entries:"
        )
        print("")

        printSamples(
          falseCallsSample,
          numFalseCalls,
          "False calls:",
          n ⇒ s"First $n false calls:"
        )
        print("")
    }
}
