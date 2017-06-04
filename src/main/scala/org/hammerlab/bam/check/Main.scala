package org.hammerlab.bam.check

import caseapp._
import grizzled.slf4j.Logging
import org.apache.spark.{ SparkConf, SparkContext }
import org.hammerlab.bam.check.full.error.Registrar

case class Args(@ExtraName("b") bamFile: String,
                @ExtraName("k") blocksFile: Option[String] = None,
                @ExtraName("r") recordsFile: Option[String] = None,
                @ExtraName("n") numBlocks: Option[Int] = None,
                @ExtraName("w") blocksWhitelist: Option[String] = None,
                @ExtraName("p") blocksPerPartition: Int = 20,
                @ExtraName("e") eager: Boolean = false)

sealed trait PosResult
  trait True extends PosResult
  trait False extends PosResult

sealed trait IsReadStart
  trait Positive extends IsReadStart
  trait Negative extends IsReadStart

trait TruePositive extends True with Positive
trait TrueNegative extends True with Negative
trait FalsePositive extends False with Positive
trait FalseNegative extends False with Negative

object Main
  extends CaseApp[Args]
    with Logging {

  /**
   * Entry-point delegated to by [[caseapp]]'s [[main]]; computes [[Result]] and prints some statistics to stdout.
   */
  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {

    val sparkConf = new SparkConf()

    sparkConf.setIfMissing(
      "spark.kryo.registrator",
      classOf[Registrar].getName
    )

    val sc = new SparkContext(sparkConf)

    if (args.eager) {
      eager.Run(sc, args)
    } else {
      full.Run(sc, args)
    }
  }
}
