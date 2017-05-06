package org.hammerlab.hadoop_bam.bgzf

case class VirtualPos(blockPos: Long, offset: Int) {
  override def toString: String = s"$blockPos:$offset"
}

object VirtualPos {
  def apply(vpos: Long): VirtualPos = VirtualPos(vpos >>> 16, (vpos & 0xffff).toInt)

  implicit val ord =
    Ordering.by[VirtualPos, (Long, Int)](vp ⇒ vp.blockPos → vp.offset)
}
