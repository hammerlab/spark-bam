package org.hammerlab.bam.spark.load

import org.hammerlab.bam.test.resources.bam5k
import org.hammerlab.spark.test.suite.SparkSuite

/**
 * Simple examples calling the BAM-loading API
 */
class APITest
  extends SparkSuite {

  test("sample load calls") {

    import org.hammerlab.bam.spark._

    val path = bam5k

    sc.loadBam(path)

    import org.hammerlab.bytes._
    sc.loadBam(path, splitSize = 32.MB)
    sc.loadBam(path, splitSize = 1 << 25)
  }

}
