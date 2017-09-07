package org.hammerlab.bam.spark.load

import org.hammerlab.bam.spark._
import org.hammerlab.bgzf.block.HeaderParseException
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File

class LoadSamAsBamFails
  extends SparkSuite {
  test("load") {
    intercept[HeaderParseException] {
      sc.loadBam(File("5k.sam"))
    }
    .getMessage should be(
      "Position 0: 64 != 31"
    )
  }
}
