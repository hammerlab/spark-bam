package org.hammerlab.bam.check.eager

import hammerlab.path._
import org.hammerlab.bam.test.resources.{ bam1, bam1Unindexed }
import org.hammerlab.cli.app.MainSuite
import org.hammerlab.test.resources.File

class CheckBamTest
  extends MainSuite(CheckBam) {

  override def defaultOpts(outPath: Path) = Seq("-m", "200k")

  val seqdoopTCGAExpectedOutput = File("output/check-bam/1.bam")

  test("compare 1.bam") {
    checkFile(
      bam1
    )(
      seqdoopTCGAExpectedOutput
    )
  }

  test("compare 1.noblocks.bam") {
    checkFile(
      bam1Unindexed
    )(
      seqdoopTCGAExpectedOutput
    )
  }

  test("seqdoop 1.bam") {
    checkFile(
      "-u", bam1
    )(
      seqdoopTCGAExpectedOutput
    )
  }

  test("eager 1.bam") {
    check(
      "-s", bam1
    )(
      """1608257 uncompressed positions
        |583K compressed
        |Compression ratio: 2.69
        |4917 reads
        |All calls matched!
        |"""
        .stripMargin
    )
  }
}
