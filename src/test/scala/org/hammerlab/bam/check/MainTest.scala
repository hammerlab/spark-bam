package org.hammerlab.bam.check

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.resources.{ tcgaBamExcerpt, tcgaBamExcerptUnindexed }
import org.hammerlab.spark.test.suite.MainSuite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.File

class MainTest
  extends MainSuite(classOf[Registrar]) {

  def compare(path: String, expected: File): Unit = {
    val outputPath = tmpPath()

    Main.main(
      Array(
        "-m", "200k",
        "-o", outputPath.toString,
        path
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
      tcgaBamExcerpt,
      seqdoopTCGAExpectedOutput
    )
  }

  test("tcga compare no .blocks file") {
    compare(
      tcgaBamExcerptUnindexed,
      seqdoopTCGAExpectedOutput
    )
  }

  test("checks more than one read") {
    compareStr(
      File("prefix.bam"),
      "-s",
      "-i", "12100265"
    )(
      """65181 uncompressed positions
        |19.6K compressed
        |Compression ratio: 3.25
        |171 reads
        |All calls matched!
        |"""
    )
  }

  test("tcga hadoop-bam") {
    val outputPath = tmpPath()

    Main.main(
      Array(
        "-u",
        "-m", "200k",
        "-o", outputPath.toString,
        tcgaBamExcerpt
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
        tcgaBamExcerpt
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

  test("exome unmapped") {
    val outputPath = tmpPath()

    Main.main(
      Array[String](
        "-m", "1m",
        "-o", outputPath.toString,
        File("HG00096.unmapped.ILLUMINA.bwa.GBR.exome.20120522.bam")
      )
    )

    outputPath.read should be(
      """"""
      .stripMargin
    )
  }
}
