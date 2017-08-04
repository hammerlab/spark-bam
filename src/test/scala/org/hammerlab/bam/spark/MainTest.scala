package org.hammerlab.bam.spark

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bytes._
import org.hammerlab.resources.tcgaBamExcerpt
import org.hammerlab.spark.test.suite.MainSuite

class MainTest
  extends MainSuite(classOf[Registrar]) {

  def check(args: Args, expected: String): Unit = {
    val outPath = tmpPath()
    Main.run(
      args.copy(
        out = Some(outPath)
      ),
      Seq[String](
        tcgaBamExcerpt
      )
    )

    outPath.read should be(expected.stripMargin)
  }

  test("eager 470KB") {
    check(
      Args(
        splitSize = Some(470.KB)
      ),
      """Split-size distribution:
        |N: 2, μ/σ: 474291.5/2723.5
        | elems: 471568 477015
        |
        |2 splits:
        |	Split(0:45846,486847:7)
        |	Split(486847:7,963864:0)
        |"""
    )
  }

  test("seqdoop 470KB") {
    check(
      Args(
        seqdoop = true,
        splitSize = Some(470.KB)
      ),
      """Split-size distribution:
        |N: 2, μ/σ: 493351.5/5508.5
        | elems: 487843 498860
        |
        |2 splits:
        |	Split(0:45846,481280:65535)
        |	Split(486847:6,963864:65535)
        |"""
    )
  }

  test("compare 470KB") {
    check(
      Args(
        compare = true,
        splitSize = Some(470.KB)
      ),
      """2 splits differ (totals: 2, 2):
        |		Split(486847:6,963864:65535)
        |	Split(486847:7,963864:0)
        |"""
    )
  }

  test("compare 480KB") {
    check(
      Args(
        compare = true,
        splitSize = Some(480.KB)
      ),
      """All splits matched!
        |
        |Split-size distribution:
        |N: 2, μ/σ: 474291.5/21385.5
        | elems: 495677 452906
        |sorted: 452906 495677
        |
        |2 splits:
        |	Split(0:45846,510891:202)
        |	Split(510891:202,963864:0)
        |"""
    )
  }
}
