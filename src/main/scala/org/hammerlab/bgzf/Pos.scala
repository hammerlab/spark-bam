package org.hammerlab.bgzf

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
