package org.hammerlab.bam.spark.compare

import org.hammerlab.bam.spark.MainSuite
import org.hammerlab.bam.test.resources.TestBams
import org.hammerlab.paths.Path
import org.hammerlab.test.matchers.lines.Line._

class CountReadsTest
  extends MainSuite(CountReads)
    with TestBams {
  override def defaultOpts(outPath: Path) = Seq("-o", outPath)

  test("1.bam 240k") {
    checkLines(
      "-m", "240k",
      bam1
    )(
      l"spark-bam read-count time: $d",
      l"hadoop-bam read-count time: $d",
      "",
      "Read counts matched: 4917",
      ""
    )
  }

  test("1.bam 230k") {
    checkLines(
      "-m", "230k",
      bam1
    )(
      "spark-bam found 4917 reads, hadoop-bam threw exception:",
      "org.apache.spark.SparkException: Job aborted due to stage failure: Task 1 in stage 1.0 failed 1 times, most recent failure: Lost task 1.0 in stage 1.0 (TID 2, localhost, executor driver): htsjdk.samtools.SAMFormatException: SAM validation error: ERROR: Record 1, Read name , MRNM should not be set for unpaired read."
    )
  }
}
