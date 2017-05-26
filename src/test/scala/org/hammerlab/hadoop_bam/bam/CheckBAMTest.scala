package org.hammerlab.hadoop_bam.bam

import org.apache.hadoop.fs.Path
import org.hammerlab.hadoop_bam.bgzf.Pos
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File
import org.hammerlab.test.matchers.seqs.SeqMatcher.seqMatch

class CheckBAMTest
  extends SparkSuite {

  test("check") {
    val errors =
      CheckBAM.run(
        sc,
        new Path(File("2.bam")),
        Args(
          File("blocks"),
          File("records"),
          File("2.bam"),
          Some(100)
        )
      )
      .collect

    errors.length should be(0)
//    errors should be(
//      Array(
//        FalsePositive(
//          Pos(268458108, 115),
//          Pos(268458108, 116),
//          65498
//        )
//      )
//    )
  }

  test("fail") {
    val errors =
      CheckBAM.run(
        sc,
        new Path(File("2.bam")),
        Args(
          File("blocks"),
          File("records"),
          File("2.bam"),
          blocksWhitelist = Some("268458108")
        )
      )
      .collect

    errors should be(
      Array(
        FalsePositive(
          Pos(268458108, 115),
          Pos(268458108, 116),
          true
        )
      )
    )
  }

  test("fail2") {
    val errors =
      CheckBAM.run(
        sc,
        new Path(File("2.bam")),
        Args(
          File("blocks"),
          File("records"),
          File("2.bam"),
          blocksWhitelist = Some("201206175,215198728,248476903,253384328")
        )
      )
      .collect

    errors should be(
      Array(
        FalsePositive(Pos(201206175,  6940), Pos(201206175,  6941), true),
        FalsePositive(Pos(215198728, 50073), Pos(215198728, 50074), true),
        FalsePositive(Pos(248476903, 30794), Pos(248476903, 30795), true),
        FalsePositive(Pos(253384328, 55615), Pos(253384328, 55616), true)
      )
    )
  }

}
