package org.hammerlab.bgzf.block

import org.hammerlab.bam.test.resources.bam5k
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.spark.test.suite.SparkSuite

class FindBlockStartTest
  extends SparkSuite {
  test("1000") {
    val in = SeekableByteChannel(bam5k)

    FindBlockStart(
      bam5k,
      2455,
      in,
      5
    ) should be(27784)
  }
}
