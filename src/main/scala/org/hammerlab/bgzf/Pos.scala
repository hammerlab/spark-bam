package org.hammerlab.bgzf

import scala.math.max

/**
 * A "virtual position" in a BGZF file: [[blockPos]] (offset to bgzf-block-start in compressed file) and [[offset]] into
 * that block's uncompressed data.
 */
case class Pos(blockPos: Long, offset: Int)
  extends Ordered[Pos] {

  override def toString: String =
    s"$blockPos:$offset"

  override def compare(that: Pos): Int =
    blockPos.compare(that.blockPos) match {
      case 0 ⇒ offset.compare(that.offset)
      case x ⇒ x
    }

  def -(other: Pos)(implicit estimatedCompressionRatio: EstimatedCompressionRatio): Double =
    max(
      0L,
      blockPos - other.blockPos +
        ((offset - other.offset) / estimatedCompressionRatio).toLong
    )

  def toHTSJDK: Long = (blockPos << 16 | offset)
}

case class EstimatedCompressionRatio(ratio: Double)

object EstimatedCompressionRatio {
  implicit def makeEstimatedCompressionRatio(ratio: Double): EstimatedCompressionRatio =
    EstimatedCompressionRatio(ratio)
  implicit def unmakeEstimatedCompressionRatio(ratio: EstimatedCompressionRatio): Double =
    ratio.ratio

  implicit val default = EstimatedCompressionRatio(3)
}

object Pos {
  /**
   * Convenience conversion from htsjdk-style positions, which are [[Long]]s with 48 bits of block-position and 16 bits
   * of intra-block uncompressed-offset.
   */
  def apply(vpos: Long): Pos =
    Pos(
      vpos >>> 16,
      (vpos & 0xffff).toInt
    )
}
