package org.hammerlab.bam.check.seqdoop

import org.hammerlab.bam.check.simple.{ FalsePositive, Result }
import org.hammerlab.bam.check.{ Args, simple }
import org.hammerlab.bgzf.Pos
import org.hammerlab.paths.Path

class RunTest
  extends simple.RunTest {
  override def run(args: Args)(implicit path: Path): Result =
    Run(args)

  override def bamTest1FalseCalls =
    Seq(
      Pos(155201,  4462) → FalsePositive,
      Pos(155201,  4780) → FalsePositive,
      Pos(155201,  5097) → FalsePositive,
      Pos(155201,  5732) → FalsePositive,
      Pos(155201,  6365) → FalsePositive,
      Pos(155201,  6683) → FalsePositive,
      Pos(155201,  7977) → FalsePositive,
      Pos(155201,  8617) → FalsePositive,
      Pos(155201,  9580) → FalsePositive,
      Pos(155201, 10536) → FalsePositive,
      Pos(155201, 10852) → FalsePositive,
      Pos(155201, 11169) → FalsePositive,
      Pos(155201, 11808) → FalsePositive,
      Pos(155201, 13075) → FalsePositive,
      Pos(155201, 16890) → FalsePositive,
      Pos(155201, 18170) → FalsePositive,
      Pos(155201, 18443) → FalsePositive,
      Pos(155201, 19072) → FalsePositive,
      Pos(155201, 19389) → FalsePositive,
      Pos(155201, 21613) → FalsePositive,
      Pos(225622, 48936) → FalsePositive,
      Pos(225622, 49212) → FalsePositive
    )
}
