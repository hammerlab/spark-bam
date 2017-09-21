package org.hammerlab.bam.spark

import org.hammerlab.bam.test.resources.bam1
import org.hammerlab.cli.args.OutputArgs
import org.hammerlab.spark.test.suite.MainSuite

class MainTest
  extends MainSuite {

  def check(args: String*)(expected: String): Unit = {
    val outPath = tmpPath()

    Main.main(
      args.toArray ++
        Array(
          "-o", outPath.toString,
          bam1.toString
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
        bam1.toString
      )
    )

    outPath.read should be(expected.stripMargin)
  }

  test("eager 230KB") {
    check(
      "-s",
      "-m", "230k"
    )(
      """Split-size distribution:
        |N: 3, μ/σ: 194067/57877.4, med/mad: 224301/20521
        | elems: 224301 244822 113078
        |sorted: 113078 224301 244822
        |
        |3 splits:
        |	0:45846-239479:312
        |	239479:312-484396:25
        |	484396:25-597482:0
        |"""
    )
  }

  test("seqdoop 230KB") {
    check(
      "-u",
      "-m", "230k"
    )(
      """Split-size distribution:
        |N: 3, μ/σ: 210102.3/53357.5, med/mad: 242083/11219
        | elems: 242083 253302 134922
        |sorted: 134922 242083 253302
        |
        |3 splits:
        |	0:45846-235520:65535
        |	239479:311-471040:65535
        |	484396:25-597482:65535
        |"""
    )
  }

  test("compare 230KB") {
    check(
      "-m", "230k"
    )(
      """2 splits differ (totals: 3, 3):
        |		239479:311-471040:65535
        |	239479:312-484396:25
        |"""
    )
  }

  test("compare 240KB") {
    check(
      "-m", "240k"
    )(
      """All splits matched!
        |
        |Split-size distribution:
        |N: 3, μ/σ: 194067/74433.1, med/mad: 244941/3497
        | elems: 248438 244941 88822
        |sorted: 88822 244941 248438
        |
        |3 splits:
        |	0:45846-263656:191
        |	263656:191-508565:287
        |	508565:287-597482:0
        |"""
    )
  }
}
