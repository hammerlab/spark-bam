package org.hammerlab.bgzf

import caseapp.core.default.Default

case class EstimatedCompressionRatio(ratio: Double)

object EstimatedCompressionRatio {
  implicit def makeEstimatedCompressionRatio(ratio: Double): EstimatedCompressionRatio =
    EstimatedCompressionRatio(ratio)
  implicit def unmakeEstimatedCompressionRatio(ratio: EstimatedCompressionRatio): Double =
    ratio.ratio

  implicit val default: EstimatedCompressionRatio = 3.0
  implicit val defaultParam = Default(default)
}
