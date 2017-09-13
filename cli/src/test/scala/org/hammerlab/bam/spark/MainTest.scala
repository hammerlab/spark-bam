package org.hammerlab.bam.spark

import org.hammerlab.args.OutputArgs
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.test.resources.tcgaBamExcerpt
import org.hammerlab.spark.test.suite.MainSuite

class MainTest
  extends MainSuite(classOf[Registrar]) {

  def check(args: String*)(expected: String): Unit = {
    val outPath = tmpPath()

    Main.main(
      args.toArray ++
        Array(
          "-o", outPath.toString,
          tcgaBamExcerpt.toString
        )
    )

    outPath.read should be(expected.stripMargin)
  }

  def check(args: Args, expected: String): Unit = {
    val outPath = tmpPath()
    Main.run(
      args.copy(
        output =
          OutputArgs(
            outputPath = Some(outPath)
          )
      ),
      Seq(
        tcgaBamExcerpt.toString
      )
    )

    outPath.read should be(expected.stripMargin)
  }

  test("eager 470KB") {
    check(
      "-s",
      "-m", "470k"
    )(
      """Split-size distribution:
        |N: 2, μ/σ: 474291.5/2723.5
        | elems: 471568 477015
        |
        |2 splits:
        |	0:45846-486847:7
        |	486847:7-963864:0
        |"""
    )
  }

  test("seqdoop 470KB") {
    check(
      "-u",
      "-m", "470k"
    )(
      """Split-size distribution:
        |N: 2, μ/σ: 493351.5/5508.5
        | elems: 487843 498860
        |
        |2 splits:
        |	0:45846-481280:65535
        |	486847:6-963864:65535
        |"""
    )
  }

  test("compare 470KB") {
    check(
      "-m", "470k"
    )(
      """2 splits differ (totals: 2, 2):
        |		486847:6-963864:65535
        |	486847:7-963864:0
        |"""
    )
  }

  test("compare 480KB") {
    check(
      "-m", "480k"
    )(
      """All splits matched!
        |
        |Split-size distribution:
        |N: 2, μ/σ: 474291.5/21385.5
        | elems: 495677 452906
        |sorted: 452906 495677
        |
        |2 splits:
        |	0:45846-510891:202
        |	510891:202-963864:0
        |"""
    )
  }
}
