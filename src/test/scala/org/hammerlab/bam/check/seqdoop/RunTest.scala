package org.hammerlab.bam.check.seqdoop

import org.hammerlab.bam.check.simple.FalsePositive
import org.hammerlab.bam.check.{ Args, Result, simple }
import org.hammerlab.bgzf.Pos
import org.hammerlab.paths.Path

class RunTest
  extends simple.RunTest {
  override def run(args: Args)(implicit path: Path): Result =
    Run(args)

  override def bamTest1FalseCalls =
    Seq(
      Pos( 39374, 30965) → FalsePositive,
      Pos(366151, 51533) → FalsePositive,
      Pos(391261, 35390) → FalsePositive,
      Pos(463275, 65228) → FalsePositive,
      Pos(486847,     6) → FalsePositive,
      Pos(731617, 46202) → FalsePositive,
      Pos(755781, 56269) → FalsePositive,
      Pos(780685, 49167) → FalsePositive,
      Pos(855668, 64691) → FalsePositive
    )
}
