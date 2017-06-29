package org.hammerlab.bam.check.simple

import org.hammerlab.bam.check.{ Args, False }
import org.hammerlab.bgzf.Pos
import org.hammerlab.hadoop.Path
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File
import org.hammerlab.test.matchers.seqs.SeqMatcher.seqMatch

trait RunTest
  extends SparkSuite {

  def run(args: Args)(implicit path: Path): Result

  def check(args: Args,
            expectedNumPositions: Int,
            expectedReadStarts: Int,
            expectedFalseCalls: Seq[(Pos, False)] = Nil)(implicit path: Path): Unit = {

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

    falseCalls.collect().toSeq should seqMatch(expectedFalseCalls)

    numReadStarts should be(expectedReadStarts)
  }

  {
    implicit val path: Path = Path(File("5k.bam"))

    test("5k.bam header block") {
      check(
        Args(
          numBlocks = Some(1)
        ),
        5650,
        0
      )
    }

    test("5k.bam 2nd block of reads") {
      check(
        Args(
          blocksWhitelist = Some("27784")
        ),
        64902,
        101
      )
    }
    test("5k.bam 10 blocks") {
      check(
        Args(
          numBlocks = Some(10)
        ),
        590166,
        918
      )
    }

    test("5k.bam all") {
      check(
        Args(),
        3139404,
        4910
      )
    }
  }

  test("1.2205029-2209029.bam") {
    check(
      Args(),
      1317661,
      4000,
      bamTest1FalseCalls
    )(
      Path(File("1.2205029-2209029.bam"))
    )
  }

  def bamTest1FalseCalls: Seq[(Pos, False)] = Nil
}
