package org.hammerlab.bam.spark.compare

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.test.resources._
import org.hammerlab.spark.test.suite.MainSuite
import org.hammerlab.test.linesMatch
import org.hammerlab.test.matchers.lines.Line._
import org.hammerlab.test.matchers.lines.{ Chars, Digits }

class MainTest
  extends MainSuite(classOf[Registrar]) {

  val ratio = Chars("0123456789.")
  val elemsOrSorted = Chars(" delmorst")

  test("230KB, 2 bams") {
    val outPath = tmpPath()
    val bamsPath = tmpPath()
    bamsPath.writeLines(
      Seq(
        bam1.toString,
        bam2.toString
      )
    )
    Main.main(
      Array(
        "-m", "230k",
        "-o", outPath.toString,
        bamsPath.toString
      )
    )

    outPath.read should linesMatch(
      "1 of 2 BAMs' splits didn't match (totals: 6, 6; 1, 1 unmatched)",
      "",
      "Total split-computation time:",
      "	hadoop-bam:	" ++ Digits,
      "	spark-bam:	" ++ Digits,
      "",
      "Ratios:",
      "N: 2, μ/σ: " ++ ratio ++ "/" ++ ratio,
      elemsOrSorted ++ ": " ++ ratio ++ " " ++ ratio,
      "",
      "	1.bam: 2 splits differ (totals: 3, 3; mismatched: 1, 1):",
      "			239479:311-471040:65535",
      "		239479:312-484396:25",
      "",
      ""
    )
  }

  test("100KB, 1 bam, no errors") {
    val outPath = tmpPath()
    val bamsPath = tmpPath()
    bamsPath.write(bam2.toString)
    Main.main(
      Array(
        "-m", "100k",
        "-o", outPath.toString,
        bamsPath.toString
      )
    )

    outPath.read should linesMatch(
      "All 1 BAMs' splits (totals: 6, 6) matched!",
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
