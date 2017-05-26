package org.hammerlab.hadoop_bam.bam

import org.hammerlab.hadoop_bam.bgzf.Pos
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class PosStreamTest
  extends Suite {

  test("simple") {
    PosStream(File("2.bam")).take(10).toList should be(
      Seq(
        Pos(0, 45846),
        Pos(0, 46163),
        Pos(0, 46439),
        Pos(0, 46760),
        Pos(0, 47083),
        Pos(0, 47400),
        Pos(0, 47716),
        Pos(0, 48033),
        Pos(0, 48351),
        Pos(0, 48679)
      )
    )
  }
}
