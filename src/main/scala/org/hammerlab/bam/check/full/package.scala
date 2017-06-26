package org.hammerlab.bam.check

import org.hammerlab.bam.check
import org.hammerlab.bam.check.full.error.Flags

package object full {

  /**
   * At each position, this package emits an optional [[Flags]] indicating which BAM-record checks failed; if none, then
   * [[None]] is emitted.
   */
  type Call = Option[Flags]

  /**
   * Turn an [[Option[Flags]]] ([[Call]]) into a [[PosResult]] whcih stores [[Flags]] info for all positions that were
   * ruled out as read-record-boundaries.
   */
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

  /**
   * [[PosResult]] "implementation" which attaches full [[Flags]] info to [[Negative]] calls (both [[True]] and
   * [[False]]).
   */
  sealed trait PosResult

  case object  TruePositive extends PosResult with  check.TruePositive
  case object FalsePositive extends PosResult with check.FalsePositive

  case class  TrueNegative(flags: Flags) extends PosResult with  check.TrueNegative
  case class FalseNegative(flags: Flags) extends PosResult with check.FalseNegative
}
