package org.hammerlab.bgzf

import cats.Show
import cats.Show.show

import scala.math.max

/**
 * A "virtual position" in a BGZF file: [[blockPos]] (offset to bgzf-block-start in compressed file) and [[offset]] into
 * that block's uncompressed data.
 */
case class Pos(blockPos: Long, offset: Int) {

  override def toString: String =
    s"$blockPos:$offset"

  def -(other: Pos)(implicit ratio: EstimatedCompressionRatio): Double =
    max(
      0L,
      blockPos - other.blockPos +
        ((offset - other.offset) / ratio).toLong
    )

  def toHTSJDK: Long = (blockPos << 16 | offset)
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

  implicit val showPos: Show[Pos] = show { _.toString }

  // Ordering brings in some undesirable implicit interactions / divergences; redo some pieces of it here
  implicit val ord: Ordering[Pos] = Ordering.by(pos ⇒ pos.blockPos → pos.offset)
  implicit def mkOrderingOps(pos: Pos): ord.Ops = new ord.Ops(pos)
}
