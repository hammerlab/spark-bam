package org.hammerlab.bgzf.block

import org.hammerlab.bgzf.Pos

case class PosIterator(block: Long,
                       uncompressedSize: Int)
  extends Iterator[Pos] {
  var up = 0
  override def hasNext: Boolean = up < uncompressedSize
  override def next(): Pos = {
    val pos = Pos(block, up)
    up += 1
    pos
  }
}

object PosIterator {
  implicit def apply(metadata: Metadata): PosIterator =
    PosIterator(
      metadata.start,
      metadata.uncompressedSize
    )
}
