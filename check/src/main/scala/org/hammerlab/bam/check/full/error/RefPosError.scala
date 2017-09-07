package org.hammerlab.bam.check.full.error

/**
 * Container for flags related to inconsistencies in BAM-records' "reference-sequence-index", "reference position", and
 * analogous "mate-" fields.
 */
sealed trait RefPosError {
  def negativeRefIdx: Boolean = false
  def tooLargeRefIdx: Boolean = false
  def negativeRefPos: Boolean = false
  def tooLargeRefPos: Boolean = false
}

sealed trait NegativeRefIdx extends RefPosError {
  override def negativeRefIdx = true
}
  case object NegativeRefIdx extends NegativeRefIdx
  case object NegativeRefIdxAndPos extends NegativeRefIdx with NegativeRefPos

sealed trait TooLargeRefIdx extends RefPosError {
  override def tooLargeRefIdx = true
}
  case object TooLargeRefIdx extends TooLargeRefIdx
  case object TooLargeRefIdxNegativePos extends TooLargeRefIdx with NegativeRefPos

sealed trait NegativeRefPos extends RefPosError {
  override def negativeRefPos = true
}
  case object NegativeRefPos extends NegativeRefPos

case object TooLargeRefPos extends RefPosError {
  override def tooLargeRefPos = true
}

