package org.hammerlab.bam.check.full

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.test.resources.{ TestBams, bam1Unindexed }
import org.hammerlab.paths.Path
import org.hammerlab.spark.test.suite.MainSuite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.File

class MainTest
  extends MainSuite(classOf[Registrar])
    with TestBams {

  def expected(basename: String) = File(s"output/full-check/$basename")

  def check(
      path: Path
  )(
      args: String*
  )(
      expected: File
  ): Unit = {
    val outputPath = tmpPath()

    Main.main(
      args.toArray ++
        Array(
          "-l", "10",
          "-o", outputPath.toString,
          path.toString
        )
    )

    outputPath should fileMatch(expected)
  }

  test("1.bam with indexed records") {
    check(
      bam1
    )(
      "-m", "200k"
    )(
      expected("1.bam")
    )
  }

/*
  test("1.bam without indexed records") {
    check(
      bam1Unindexed
    )(
      "-m", "200k"
    )(
      expected("1.noblocks.bam")
    )
  }
*/

  test("2.bam first block") {
    check(
      bam2
    )(
      "-i", "0"
    )(
      expected("2.bam.first")
    )
  }

  test("2.bam second block") {
    check(
      bam2
    )(
      "-i", "26169"
    )(
      expected("2.bam.second")
    )
  }

  test("2.bam 200k") {
    check(
      bam2
    )(
      "-i", "0-200k",
      "-m", "100k"
    )(
      expected("2.bam.200k")
    )
  }

/*
  test("2.bam all") {
    check(
      bam2
    )(
      // All default args
    )(
      expected("2.bam")
    )
  }
*/
}
