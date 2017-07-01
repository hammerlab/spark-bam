package org.hammerlab.bgzf.block

import org.hammerlab.hadoop.{ Configuration, Path }
import org.hammerlab.io.SeekableByteChannel
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File
import org.hammerlab.timing.Timer.time

class FindBlockStartTest
  extends SparkSuite {
  test("1000") {
    val path = Path(File("5k.bam").uri)
    val in = SeekableByteChannel(path)

    val start =
      time {
        FindBlockStart(
          path,
          2455,
          in,
          5
        )
      }

   start should be(27784)
  }
}
