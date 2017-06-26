package org.hammerlab.bam.check

import org.hammerlab.bam.check

package object simple {
  type Call = Boolean
  implicit object MakePosResult
    extends MakePosResult[Call, PosResult] {
    override def apply(call: Call, isReadStart: Boolean): PosResult =
      (call, isReadStart) match {
        case (true, true) ⇒
          TruePositive
        case (true, false) ⇒
          FalsePositive
        case (false, true) ⇒
          FalseNegative
        case (false, false) ⇒
          TrueNegative
      }
  }

  sealed trait PosResult

  case object  TruePositive extends PosResult with  check.TruePositive
  case object  TrueNegative extends PosResult with  check.TrueNegative
  case object FalsePositive extends PosResult with check.FalsePositive
  case object FalseNegative extends PosResult with check.FalseNegative
}
