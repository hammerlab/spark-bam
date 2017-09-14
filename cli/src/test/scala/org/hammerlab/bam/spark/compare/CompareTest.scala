package org.hammerlab.bam.spark.compare

import org.apache.spark.broadcast.Broadcast
import org.hammerlab.bam.check.Checker.{ MaxReadSize, ReadsToCheck, default }
import org.hammerlab.bam.spark.Split
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.BGZFBlocksToCheck
import org.hammerlab.bytes._
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.bam.test.resources.bam1
import org.hammerlab.spark.test.suite.SparkSuite
import shapeless.LabelledGeneric

class CompareTest
  extends SparkSuite {

  implicit val readsToCheck = default[ReadsToCheck]
  implicit val maxReadSize = default[MaxReadSize]
  implicit val bgzfBlocksToCheck = default[BGZFBlocksToCheck]

  val lg = LabelledGeneric[Result]

  import shapeless._, record._

  def check(actual: Result, expected: Result): Unit = {
    actual.copy(hadoopBamMS = 0, sparkBamMS = 0) should be(
      expected
    )
  }

  implicit lazy val confBroadcast: Broadcast[Configuration] = sc.broadcast(ctx)

  test("470KB") {
    implicit val splitSize = MaxSplitSize(470.KB)
    val actual = getPathResult(bam1)

    val expected =
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
        1,
        0,  // dummy value, timing values not checked
        0   // dummy value, timing values not checked
      )

    check(actual, expected)
  }

  test("235KB") {
    implicit val splitSize = MaxSplitSize(235.KB)
    check(
      getPathResult(bam1),
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
        1,
        0,  // dummy value, timing values not checked
        0   // dummy value, timing values not checked
      )
    )
  }
}
