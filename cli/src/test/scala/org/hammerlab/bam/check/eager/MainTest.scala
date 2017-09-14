package org.hammerlab.bam.check.eager

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.test.resources.{ bam1, bam1Unindexed }
import org.hammerlab.paths.Path
import org.hammerlab.spark.test.suite.MainSuite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.File

class MainTest
  extends MainSuite(classOf[Registrar]) {

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

  val seqdoopTCGAExpectedOutput = File("output/check-bam/seqdoop/tcga")

  test("tcga compare") {
    compare(
      bam1,
      seqdoopTCGAExpectedOutput
    )
  }

  test("tcga compare no .blocks file") {
    compare(
      bam1Unindexed,
      seqdoopTCGAExpectedOutput
    )
  }

  test("tcga hadoop-bam") {
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

  test("tcga spark-bam") {
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
      """2580596 uncompressed positions
        |941K compressed
        |Compression ratio: 2.68
        |7976 reads
        |All calls matched!
        |"""
      .stripMargin
    )
  }
}
