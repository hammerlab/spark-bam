package org.hammerlab.bam.check

import org.hammerlab.bam.check
import org.hammerlab.bam.check.full.error.Flags

package object full {
  type Call = Option[Flags]

  implicit object MakePosResult extends MakePosResult[Call, PosResult] {
    def apply(call: Option[Flags], isReadStart: Boolean): PosResult =
      (call, isReadStart) match {
        case (None, true) ⇒
          TruePositive
        case (None, false) ⇒
          FalsePositive
        case (Some(flags), true) ⇒
          FalseNegative(flags)
        case (Some(flags), false) ⇒
          TrueNegative(flags)
      }
  }

  sealed trait PosResult

  case object TruePositive extends PosResult with check.TruePositive
  case class TrueNegative(flags: Flags) extends PosResult with check.TrueNegative
  case object FalsePositive extends PosResult with check.FalsePositive
  case class FalseNegative(flags: Flags) extends PosResult with check.FalseNegative
}
