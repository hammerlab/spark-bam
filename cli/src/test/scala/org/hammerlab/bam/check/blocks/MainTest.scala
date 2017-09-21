package org.hammerlab.bam.check.blocks

import org.hammerlab.bam.test.resources.{ bam1, bam2 }
import org.hammerlab.paths.Path
import org.hammerlab.spark.test.suite.MainSuite

class MainTest
  extends MainSuite {

  def check(bam: Path, args: String*)(expected: String): Unit = {
    val out = tmpPath()
    Main.main(
      args.toArray ++
        Array(
          "-o", out.toString,
          bam.toString
        )
    )

    out.read should be(expected.stripMargin)
  }

  test("1.bam") {
    check(
      bam1
    )(
      """First read-position mis-matchined in 1 of 25 BGZF blocks
        |
        |25871 of 597482 (0.043300049206503294) compressed positions would lead to bad splits
        |
        |1 mismatched blocks:
        |	239479 (prev block size: 25871):	239479:312	239479:311
        |"""
    )
  }

  test("1.bam spark-bam") {
    check(
      bam1,
      "-s"
    )(
      """First read-position matched in 25 BGZF blocks totaling 583KB (compressed)
        |"""
    )
  }

  test("1.bam hadoop-bam") {
    check(
      bam1,
      "-u"
    )(
      """First read-position mis-matchined in 1 of 25 BGZF blocks
        |
        |25871 of 597482 (0.043300049206503294) compressed positions would lead to bad splits
        |
        |1 mismatched blocks:
        |	239479 (prev block size: 25871):	239479:312	239479:311
        |"""
    )
  }

  test("2.bam") {
    check(
      bam2
    )(
      """First read-position matched in 25 BGZF blocks totaling 519KB (compressed)
        |"""
    )
  }

}
