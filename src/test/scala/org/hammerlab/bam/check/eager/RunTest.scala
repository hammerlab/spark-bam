package org.hammerlab.bam.check.eager

import org.hammerlab.bam.check.{ Args, Positive, True }
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class RunTest
  extends SparkSuite {
  def check(args: Args,
            expectedNumPositions: Int,
            expectedReadStarts: Int): Unit = {

    val result = Run(sc, args)

    val Result(
      numPositions,
      _,
      numFalseCalls,
      falseCalls
    ) =
      result

    numPositions should be(expectedNumPositions)

    numFalseCalls should be(0)

    result
      .results
      .values
      .filter {
        case _: Positive ⇒ true
        case _ ⇒ false
      }
      .count should be(expectedReadStarts)
  }

  test("5k.bam header block") {
    check(
      Args(
        File("5k.bam"),
        numBlocks = Some(1)
      ),
      5650,
      0
    )
  }

  test("5k.bam 2nd block of reads") {
    check(
      Args(
        File("5k.bam"),
        blocksWhitelist = Some("27784")
      ),
      64902,
      101
    )
  }
  test("5k.bam 10 blocks") {
    check(
      Args(
        File("5k.bam"),
        numBlocks = Some(10)
      ),
      590166,
      918
    )
  }

  test("5k.bam all") {
    check(
      Args(
        File("5k.bam")
      ),
      3139404,
      4910
    )
  }
}
