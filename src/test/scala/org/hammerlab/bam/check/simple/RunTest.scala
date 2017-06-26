package org.hammerlab.bam.check.simple

import org.hammerlab.bam.check.{ Args, False }
import org.hammerlab.bgzf.Pos
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File

trait RunTest
  extends SparkSuite {

  def run(args: Args): Result

  def check(args: Args,
            expectedNumPositions: Int,
            expectedReadStarts: Int,
            expectedFalseCalls: Seq[(Pos, False)] = Nil): Unit = {

    val result = run(args)

    val Result(
      numPositions,
      _,
      numFalseCalls,
      falseCalls,
      numReadStarts,
      _
    ) =
      result

    numPositions should be(expectedNumPositions)

    falseCalls.collect() should be(expectedFalseCalls)

    numReadStarts should be(expectedReadStarts)
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

  test("1.2205029-2209029.bam") {
    check(
      Args(
        File("1.2205029-2209029.bam")
      ),
      1317661,
      4000,
      bamTest1FalseCalls
    )
  }

  def bamTest1FalseCalls: Seq[(Pos, False)] = Nil
}
