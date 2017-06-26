package org.hammerlab.bam.check.seqdoop

import org.hammerlab.bam.check.simple.{ Result, FalsePositive }
import org.hammerlab.bam.check.{ Args, simple }
import org.hammerlab.bgzf.Pos

class RunTest
  extends simple.RunTest {
  override def run(args: Args): Result =
    Run(sc, args)

  override def bamTest1FalseCalls =
//    Nil
    Seq(
      Pos(130149, 34000) → FalsePositive,
      Pos(155201,  4462) → FalsePositive,
      Pos(155201,  4780) → FalsePositive,
      Pos(155201,  5097) → FalsePositive,
      Pos(155201,  5732) → FalsePositive,
      Pos(155201,  6365) → FalsePositive,
      Pos(155201,  6683) → FalsePositive,
      Pos(155201,  6999) → FalsePositive,
      Pos(155201,  7324) → FalsePositive,
      Pos(155201,  7650) → FalsePositive,
      Pos(155201,  7977) → FalsePositive,
      Pos(155201,  8295) → FalsePositive,
      Pos(155201,  8617) → FalsePositive,
      Pos(155201,  8934) → FalsePositive,
      Pos(155201,  9580) → FalsePositive,
      Pos(155201, 10213) → FalsePositive,
      Pos(155201, 10536) → FalsePositive,
      Pos(155201, 10852) → FalsePositive,
      Pos(155201, 11169) → FalsePositive,
      Pos(155201, 11486) → FalsePositive,
      Pos(155201, 11808) → FalsePositive,
      Pos(155201, 13075) → FalsePositive,
      Pos(155201, 16890) → FalsePositive,
      Pos(155201, 18170) → FalsePositive,
      Pos(155201, 18443) → FalsePositive,
      Pos(155201, 19072) → FalsePositive,
      Pos(155201, 19389) → FalsePositive,
      Pos(155201, 21613) → FalsePositive,
      Pos(155201, 21931) → FalsePositive,
      Pos(155201, 29560) → FalsePositive,
      Pos(225622, 48936) → FalsePositive,
      Pos(225622, 49212) → FalsePositive
    )

//    Seq(
//      Pos(441192, 37166) → FalsePositive,
//      Pos(441192, 38123) → FalsePositive,
//      Pos(465250,  7940) → FalsePositive,
//      Pos(465250, 17143) → FalsePositive,
//      Pos(465250, 32387) → FalsePositive,
//      Pos(465250, 32983) → FalsePositive,
//      Pos(465250, 33301) → FalsePositive,
//      Pos(465250, 33618) → FalsePositive

      //      Pos(155201, 18170) → FalsePositive,
//      Pos(225622, 48936) → FalsePositive

  //    )
}
