package org.hammerlab.bgzf.block

import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.resources.bam5k
import org.hammerlab.spark.test.suite.SparkSuite

class FindBlockStartTest
  extends SparkSuite {
  test("1000") {
    val path = bam5k.path
    val in = SeekableByteChannel(path)

    FindBlockStart(
      path,
      2455,
      in,
      5
    ) should be(27784)
  }
}
