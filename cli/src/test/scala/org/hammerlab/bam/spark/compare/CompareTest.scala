package org.hammerlab.bam.spark.compare

import org.apache.spark.broadcast.Broadcast
import org.hammerlab.bam.check.Checker.default
import org.hammerlab.bam.check.{ MaxReadSize, ReadsToCheck }
import org.hammerlab.bam.spark.Split
import org.hammerlab.bam.test.resources.bam1
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.BGZFBlocksToCheck
import org.hammerlab.bytes._
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.spark.test.suite.SparkSuite
import shapeless.LabelledGeneric

class CompareTest
  extends SparkSuite {

  implicit val readsToCheck      = default[ReadsToCheck]
  implicit val maxReadSize       = default[MaxReadSize]
  implicit val bgzfBlocksToCheck = default[BGZFBlocksToCheck]

  val lg = LabelledGeneric[Result]

  def check(actual: Result, expected: Result): Unit = {
    actual.copy(hadoopBamMS = 0, sparkBamMS = 0) should be(
      expected
    )
  }

  implicit lazy val confBroadcast: Broadcast[Configuration] = sc.broadcast(ctx)

  test("230kb") {
    implicit val splitSize = MaxSplitSize(230.KB)
    val actual = Result(bam1)

    val expected =
      Result(
        3,
        3,
        Vector(
          Right(
            Split(
              Pos(239479,   311),
              Pos(471040, 65535)
            )
          ),
          Left(
            Split(
              Pos(239479,   312),
              Pos(484396,    25)
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

  test("115KB") {
    implicit val splitSize = MaxSplitSize(115.KB)
    check(
      Result(bam1),
      Result(
        5,
        5,
        Vector(
          Right(
            Split(
              Pos(239479,   311),
              Pos(353280, 65535)
            )
          ),
          Left(
            Split(
              Pos(239479,   312),
              Pos(361204,    42)
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
