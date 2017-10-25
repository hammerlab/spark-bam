package org.hammerlab.bam.spark.compare

import org.hammerlab.bam.test.resources._
import org.hammerlab.cli.app.MainSuite
import org.hammerlab.test.matchers.lines.Chars
import org.hammerlab.test.matchers.lines.Line._

class CompareSplitsTest
  extends MainSuite(CompareSplits) {

  val ratio = Chars("0123456789.")
  val elemsOrSorted = Chars(" delmorst")

  test("230KB, 2 bams") {
    val bamsPath = tmpPath()
    bamsPath.writeLines(
      Seq(
        bam1.toString,
        bam2.toString
      )
    )

    checkAllLines(
      "-m", "230k",
      bamsPath
    )(
      "1 of 2 BAMs' splits didn't match (totals: 6, 6; 1, 1 unmatched)",
      "",
      "Total split-computation time:",
      l"	hadoop-bam:	$d",
      l"	spark-bam:	$d",
      "",
      "Ratios:",
      l"N: 2, μ/σ: $ratio/$ratio",
      l"$elemsOrSorted: $ratio $ratio",
      "",
      "	1.bam: 2 splits differ (totals: 3, 3; mismatched: 1, 1):",
      "			239479:311-471040:65535",
      "		239479:312-484396:25",
      "",
      ""
    )
  }

  test("100KB, 1 bam, no errors") {
    val bamsPath = tmpPath()
    bamsPath.write(bam2.toString)

    checkAllLines(
      "-m", "100k",
      bamsPath
    )(
      "All 1 BAMs' splits (totals: 6, 6) matched!",
      "",
      "Total split-computation time:",
      l"	hadoop-bam:	$d",
      l"	spark-bam:	$d",
      "",
      l"Ratio: $ratio",
      "",
      ""
    )
  }
}
