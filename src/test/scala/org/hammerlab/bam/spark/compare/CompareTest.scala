package org.hammerlab.bam.spark.compare

import org.hammerlab.bam.spark.Split
import org.hammerlab.bgzf.Pos
import org.hammerlab.bytes._
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.resources.tcgaBamExcerpt
import org.hammerlab.spark.test.suite.SparkSuite

class CompareTest
  extends SparkSuite {
  test("470KB") {
    implicit val splitSize = MaxSplitSize(470.KB)
    getPathResult(tcgaBamExcerpt) should be(
      Result(
        2,
        2,
        Vector(
          Right(
            Split(
              Pos(486847,     6),
              Pos(963864, 65535)
            )
          ),
          Left(
            Split(
              Pos(486847,     7),
              Pos(963864,     0)
            )
          )
        ),
        1,
        1
      )
    )
  }

  test("235KB") {
    implicit val splitSize = MaxSplitSize(235.KB)
    getPathResult(tcgaBamExcerpt) should be(
      Result(
        4,
        4,
        Vector(
          Right(
            Split(
              Pos(486847,     6),
              Pos(721920, 65535)
            )
          ),
          Left(
            Split(
              Pos(486847,     7),
              Pos(731617,    36)
            )
          )
        ),
        1,
        1
      )
    )
  }
}
