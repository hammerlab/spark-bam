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
   * [[PosResult]] "implementation" which attaches full [[Flags]] info to [[Negative]] calls (both [[True]] and
   * [[False]]).
   */
  sealed trait PosResult

  case object  TruePositive extends PosResult with  check.TruePositive
  case object FalsePositive extends PosResult with check.FalsePositive

  case class  TrueNegative(flags: Flags) extends PosResult with  check.TrueNegative
  case class FalseNegative(flags: Flags) extends PosResult with check.FalseNegative
}
