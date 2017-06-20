package org.hammerlab.bam.check

import caseapp.{ ExtraName ⇒ O, _ }
import grizzled.slf4j.Logging
import org.apache.spark.{ SparkConf, SparkContext }
import org.hammerlab.bam.check.full.error.Registrar

/**
 * CLI for [[Main]]: check every (bgzf-decompressed) byte-position in a BAM file with a [[Checker]] and compare the
 * results to the true read-start positions.
 *
 * Requires the BAM to have been indexed prior to running by [[org.hammerlab.bgzf.index.IndexBlocks]] and
 * [[org.hammerlab.bam.index.IndexRecords]].
 *
 * @param bamFile BAM file to check all (uncompressed) positions of
 * @param blocksFile file with bgzf-block-start positions as output by [[org.hammerlab.bgzf.index.IndexBlocks]]
 * @param recordsFile file with BAM-record-start positions as output by [[org.hammerlab.bam.index.IndexRecords]]
 * @param numBlocks if set, only check the first [[numBlocks]] bgzf blocks of [[bamFile]]
 * @param blocksWhitelist if set, only process the bgzf blocks at these positions (comma-seperated)
 * @param blocksPerPartition process this many blocks in each partition
 * @param eager if set, run [[org.hammerlab.bam.check.eager.Run]], which marks a position as "negative" and returns as
 *              soon as any check fails. Default: [[org.hammerlab.bam.check.full.Run]], which performs as many checks
 *              as possible and aggregates statistics about how many times each check participates in ruling out a given
 *              position.
 */
case class Args(@O("b") bamFile: String,
                @O("k") blocksFile: Option[String] = None,
                @O("r") recordsFile: Option[String] = None,
                @O("n") numBlocks: Option[Int] = None,
                @O("w") blocksWhitelist: Option[String] = None,
                @O("p") blocksPerPartition: Int = 20,
                @O("e") eager: Boolean = false)

object Main
  extends CaseApp[Args]
    with Logging {

  /**
   * Entry-point delegated to by [[caseapp]]'s [[main]]; delegates to a [[Run]] implementation indicated by
   * [[Args.eager]].
   */
  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {

    val sparkConf = new SparkConf()

    sparkConf.setIfMissing(
      "spark.kryo.registrator",
      classOf[Registrar].getName
    )

    sparkConf.setIfMissing(
      "spark.kryo.registrationRequired",
      "true"
    )

    val sc = new SparkContext(sparkConf)

    if (args.eager)
      eager.Run(sc, args)
    else
      full.Run(sc, args)
  }
}
