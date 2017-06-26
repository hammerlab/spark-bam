package org.hammerlab.bam.check

/**
 * Trait hierarchy for recording {[[True]],[[False]]} x {[[Positive]],[[Negative]]} information.
 */
sealed trait PosResult
  sealed trait True extends PosResult
  sealed trait False extends PosResult

  sealed trait Positive extends PosResult
  sealed trait Negative extends PosResult

  trait  TruePositive extends  True with Positive
  trait  TrueNegative extends  True with Negative
  trait FalsePositive extends False with Negative
  trait FalseNegative extends False with Positive

case object  TruePositive extends  TruePositive
case object  TrueNegative extends  TrueNegative
case object FalsePositive extends FalsePositive
case object FalseNegative extends FalseNegative
