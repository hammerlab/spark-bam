package org.hammerlab.bam.spark.compare

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.resources._
import org.hammerlab.spark.test.suite.MainSuite

class MainTest
  extends MainSuite(classOf[Registrar]) {

  test("470KB, 2 bams") {
    val outPath = tmpPath()
    val bamsPath = tmpPath()
    bamsPath.writeLines(
      Seq(
        tcgaBamExcerpt,
        bam5k
      )
    )
    Main.main(
      Array(
        "-m", "470k",
        "-o", outPath.toString,
        "-f", bamsPath.toString
      )
    )

    outPath.read should be(
      """1 of 2 BAMs' splits didn't match (totals: 5, 5; 1, 1 unmatched):
        |
        |	1.2203053-2211029.bam: 2 splits differ (totals: 2, 2; mismatched: 1, 1):
        |				Split(486847:6,963864:65535)
        |			Split(486847:7,963864:0)
        |
        |"""
      .stripMargin)
  }

  test("400KB, 1 bam, no errors") {
    val outPath = tmpPath()
    val bamsPath = tmpPath()
    bamsPath.write(bam5k)
    Main.main(
      Array(
        "-m", "400k",
        "-o", outPath.toString,
        "-f", bamsPath.toString
      )
    )

    outPath.read should be(
      """"""
      .stripMargin)
  }
}
