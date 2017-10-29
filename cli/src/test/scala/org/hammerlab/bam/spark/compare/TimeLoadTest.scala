package org.hammerlab.bam.spark.compare

import org.hammerlab.bam.test.resources.TestBams
import org.hammerlab.cli.app.MainSuite
import org.hammerlab.test.matchers.lines.Line._

class TimeLoadTest
  extends MainSuite(TimeLoad)
    with TestBams {

  test("1.bam 230k") {
    checkFirstLines(
      "-m", "230k",
      bam1
    )(
      l"spark-bam first-read collection time: $d",
      "",
      "spark-bam collected 3 partitions' first-reads",
      "hadoop-bam threw an exception:",
      l"org.apache.spark.SparkException: Job aborted due to stage failure: Task 1 in stage 0.0 failed 1 times, most recent failure: Lost task 1.0 in stage 0.0 (TID $d, localhost, executor driver): htsjdk.samtools.SAMFormatException: SAM validation error: ERROR: Record 1, Read name , MRNM should not be set for unpaired read."
    )
  }

  test("1.bam 240k") {
    checkAllLines(
      "-m", "240k",
      bam1
    )(
      l"spark-bam first-read collection time: $d",
      l"hadoop-bam first-read collection time: $d",
      "",
      "All 3 partition-start reads matched",
      ""
    )
  }
}
