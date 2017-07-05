package org.hammerlab.bam.check.simple

import org.hammerlab.bam.check.{ Args, False }
import org.hammerlab.bgzf.Pos
import org.hammerlab.paths.Path
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
      _,
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
    implicit val path: Path = File("5k.bam").path

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

  test("1.2203053-2211029.bam") {
    check(
      Args(),
      2580596,
      7976,
      bamTest1FalseCalls
    )(
      File("1.2203053-2211029.bam").path
    )
  }

  def bamTest1FalseCalls: Seq[(Pos, False)] = Nil
}
