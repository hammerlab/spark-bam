package org.hammerlab.bam.spark.compare

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.resources._
import org.hammerlab.spark.test.suite.MainSuite
import org.hammerlab.test.linesMatch
import org.hammerlab.test.matchers.lines.{ Chars, Digits }
import org.hammerlab.test.matchers.lines.Line._

class MainTest
  extends MainSuite(classOf[Registrar]) {

  val ratio = Chars("0123456789.")

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
        bamsPath.toString
      )
    )

    outPath.read should linesMatch(
      "1 of 2 BAMs' splits didn't match (totals: 5, 5; 1, 1 unmatched)",
      "",
      "Total split-computation time:",
      "	hadoop-bam:	" ++ Digits,
      "	spark-bam:	" ++ Digits,
      "",
      "Ratios:",
      "N: 2, μ/σ: " ++ ratio ++ "/" ++ ratio,
      " elems: " ++ ratio ++ " " ++ ratio,
      "",
      "	1.2203053-2211029.bam: 2 splits differ (totals: 2, 2; mismatched: 1, 1):",
      "				486847:6-963864:65535",
      "			486847:7-963864:0",
      "",
      ""
    )
  }

  test("400KB, 1 bam, no errors") {
    val outPath = tmpPath()
    val bamsPath = tmpPath()
    bamsPath.write(bam5k)
    Main.main(
      Array(
        "-m", "400k",
        "-o", outPath.toString,
        bamsPath.toString
      )
    )

    outPath.read should linesMatch(
      "All 1 BAMs' splits (totals: 3, 3) matched!",
      "",
      "Total split-computation time:",
      "	hadoop-bam:	" ++ Digits,
      "	spark-bam:	" ++ Digits,
      "",
      "Ratio: " ++ ratio,
      "",
      ""
    )
  }
}
