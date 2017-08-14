package org.hammerlab.bam.check

import org.hammerlab.bam.check

package object simple {
  type Call = Boolean

  sealed trait PosResult {
    self: check.PosResult ⇒
  }

  case object  TruePositive extends PosResult with  check.TruePositive
  case object  TrueNegative extends PosResult with  check.TrueNegative
  case object FalsePositive extends PosResult with check.FalsePositive
  case object FalseNegative extends PosResult with check.FalseNegative
}
