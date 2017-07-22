package org.hammerlab.bam.check.simple

import org.hammerlab.bam.check.{ Args, False, Result ⇒ Res }
import org.hammerlab.bgzf.Pos
import org.hammerlab.paths.Path
import org.hammerlab.resources.{ bam5k, tcgaBamExcerpt }
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.matchers.seqs.ArrMatcher.arrMatch

trait RunTest
  extends SparkSuite {

  def run(args: Args)(implicit path: Path): Res

  def check(args: Args,
            expectedNumPositions: Int,
            expectedReadStarts: Int,
            expectedFalseCalls: Seq[(Pos, False)] = Nil)(implicit path: Path): Unit = {

    val result = run(args)

    val Res(
      numPositions,
      _,
      falseCalls,
      numReadStarts
    ) =
      result

    numPositions should be(expectedNumPositions)

    falseCalls.collect() should arrMatch(expectedFalseCalls)

    numReadStarts should be(expectedReadStarts)
  }

  {
    implicit val path: Path = bam5k

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

  test("tcga bam") {
    check(
      Args(),
      2580596,
      7976,
      bamTest1FalseCalls
    )(
      tcgaBamExcerpt
    )
  }

  def bamTest1FalseCalls: Seq[(Pos, False)] = Nil
}
