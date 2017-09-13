package org.hammerlab.bam.spark.load

import org.hammerlab.bam.spark._
import org.hammerlab.bgzf.block.HeaderParseException
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.bam.test.resources.{ TestBams, sam5k }

class LoadSamAsBamFails
  extends SparkSuite 
    with TestBams {
  test("load") {
    intercept[HeaderParseException] {
      sc.loadBam(sam5k)
    }
    .getMessage should be(
      "Position 0: 64 != 31"
    )
  }
}
