package org.hammerlab.bgzf.block

import org.hammerlab.io.SeekableByteChannel
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File

class FindBlockStartTest
  extends SparkSuite {
  test("1000") {
    val path = File("5k.bam").path
    val in = SeekableByteChannel(path)

    FindBlockStart(
      path,
      2455,
      in,
      5
    ) should be(27784)
  }
}
