package org.hammerlab.bgzf.block

import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.spark.test.suite.SparkSuite

class FindBlockStartTest
  extends SparkSuite {
  test("1000") {
    FindBlockStart(
      bam2,
      26170,
      SeekableByteChannel(bam2),
      5
    ) should be(50249)
  }
}
