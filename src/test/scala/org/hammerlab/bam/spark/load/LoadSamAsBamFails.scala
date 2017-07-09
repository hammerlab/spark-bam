package org.hammerlab.bam.spark.load

import org.hammerlab.bam.spark._
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.firstLinesMatch
import org.hammerlab.test.matchers.lines.Line._
import org.hammerlab.test.matchers.lines.{ LineNumber, NotChar }
import org.hammerlab.test.resources.File

class LoadSamAsBamFails
  extends SparkSuite {
  test("load") {
    intercept[Exception] {
      sc.loadBam(File("5k.sam"))
    }
    .getMessage should firstLinesMatch(
      "1 uncaught exceptions thrown in parallel worker threads:",
      "	org.hammerlab.bgzf.block.HeaderSearchFailedException: file:///" ++ NotChar(':') ++ ": failed to find BGZF header in 65536 bytes from 0",
      "		at org.hammerlab.bgzf.block.FindBlockStart$.apply(FindBlockStart.scala:" ++ LineNumber ++ ")",
      "		at org.hammerlab.bam.spark.load.CanLoadBam$$anonfun$9.apply(CanLoadBam.scala:" ++ LineNumber ++ ")",
      "		at org.hammerlab.bam.spark.load.CanLoadBam$$anonfun$9.apply(CanLoadBam.scala:" ++ LineNumber ++ ")"
    )
  }
}
