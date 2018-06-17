package org.hammerlab.bam.spark.load

import org.hammerlab.bam.test.resources.TestBams
import org.hammerlab.spark.test.suite.SparkSuite
import spark_bam._

class LoadSamAsBamFails
  extends SparkSuite 
    with TestBams {
  test("load") {
    intercept[IllegalArgumentException] {
      sc.loadBam(sam2)
    }
    .getMessage should startWith(
      "Expected 'bam' extension"
    )
  }
}
