package org.hammerlab.bgzf

import caseapp.core.Default

case class EstimatedCompressionRatio(ratio: Double)

object EstimatedCompressionRatio {
  implicit def makeEstimatedCompressionRatio(ratio: Double): EstimatedCompressionRatio =
    EstimatedCompressionRatio(ratio)
  implicit def unmakeEstimatedCompressionRatio(ratio: EstimatedCompressionRatio): Double =
    ratio.ratio

  implicit val default: Default[EstimatedCompressionRatio] = Default.instance(3.0)
}
