package org.hammerlab.bam.check.full

import org.hammerlab.bam.test.resources.{ TestBams, bam1Unindexed }
import org.hammerlab.cli.app.MainSuite
import org.hammerlab.paths.Path
import org.hammerlab.test.resources.File

class FullCheckTest
  extends MainSuite(FullCheck.Main)
    with TestBams {

  override def defaultOpts(outPath: Path) = Seq("-l", "10")

  def expected(basename: String): File = File(s"output/full-check/$basename")

  test("1.bam with indexed records") {
    checkFile(
      "-m", "200k",
      bam1
    )(
      expected("1.bam")
    )
  }

  test("1.bam without indexed records") {
    checkFile(
      "-m", "200k",
      bam1Unindexed
    )(
      expected("1.noblocks.bam")
    )
  }

  test("2.bam first block") {
    checkFile(
      "-i", "0",
      bam2
    )(
      expected("2.bam.first")
    )
  }

  test("2.bam second block") {
    checkFile(
      "-i", "26169",
      bam2
    )(
      expected("2.bam.second")
    )
  }

  test("2.bam 200k") {
    checkFile(
      "-i", "0-200k",
      "-m", "100k",
      bam2
    )(
      expected("2.bam.200k")
    )
  }

  test("2.bam all") {
    checkFile(
      bam2
    )(
      expected("2.bam")
    )
  }
}
