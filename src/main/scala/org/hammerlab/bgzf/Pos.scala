package org.hammerlab.bgzf

case class Pos(blockPos: Long, offset: Int) extends Ordered[Pos] {
  override def toString: String = s"$blockPos:$offset"

  override def compare(that: Pos): Int =
    blockPos.compare(that.blockPos) match {
      case 0 ⇒offset.compare(that.offset)
      case x ⇒ x
    }
}

object Pos {
  def apply(vpos: Long): Pos = Pos(vpos >>> 16, (vpos & 0xffff).toInt)

  implicit val ord =
    Ordering.by[Pos, (Long, Int)](vp ⇒ vp.blockPos → vp.offset)
}
