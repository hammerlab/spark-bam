package org.hammerlab.bgzf

import caseapp.core.default.Default

case class EstimatedCompressionRatio(ratio: Double)

object EstimatedCompressionRatio {
  implicit def makeEstimatedCompressionRatio(ratio: Double): EstimatedCompressionRatio =
    EstimatedCompressionRatio(ratio)
  implicit def unmakeEstimatedCompressionRatio(ratio: EstimatedCompressionRatio): Double =
    ratio.ratio

  implicit val default: Default[EstimatedCompressionRatio] = Default(3.0)
}
