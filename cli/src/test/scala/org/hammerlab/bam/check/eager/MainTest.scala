package org.hammerlab.bam.check.eager

import org.hammerlab.bam.test.resources.{ bam1, bam1Unindexed }
import org.hammerlab.paths.Path
import org.hammerlab.spark.test.suite.MainSuite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.File

class MainTest
  extends MainSuite {

  def compare(path: Path, expected: File): Unit = {
    val outputPath = tmpPath()

    Main.main(
      Array(
        "-m", "200k",
        "-o", outputPath.toString,
        path.toString
      )
    )

    outputPath should fileMatch(expected)
  }

  def compareStr(path: String, args: String*)(expected: String): Unit = {
    val outputPath = tmpPath()

    Main.main(
      Array(
        "-m", "200k",
        "-o", outputPath.toString
      ) ++
        args.toArray[String] ++
        Array[String](path)
    )

    outputPath.read should be(expected.stripMargin)
  }

  val seqdoopTCGAExpectedOutput = File("output/check-bam/1.bam")

  test("compare 1.bam") {
    compare(
      bam1,
      seqdoopTCGAExpectedOutput
    )
  }

  test("compare 1.noblocks.bam") {
    compare(
      bam1Unindexed,
      seqdoopTCGAExpectedOutput
    )
  }

  test("seqdoop 1.bam") {
    val outputPath = tmpPath()

    Main.main(
      Array(
        "-u",
        "-m", "200k",
        "-o", outputPath.toString,
        bam1.toString
      )
    )

    outputPath should fileMatch(seqdoopTCGAExpectedOutput)
  }

  test("eager 1.bam") {
    val outputPath = tmpPath()

    Main.main(
      Array(
        "-s",
        "-m", "200k",
        "-o", outputPath.toString,
        bam1.toString
      )
    )

    outputPath.read should be(
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
