package org.hammerlab.bam.check

import org.hammerlab.bgzf.Pos
import org.hammerlab.iterator.SimpleBufferedIterator

/**
 * Apply a [[Checker]] to each bgzf-decompressed position in a [[block]].
 */
case class PosCallIterator[Call](block: Long,
                                 uncompressedSize: Int,
                                 checker: Checker[Call])
  extends SimpleBufferedIterator[(Pos, Call)] {

  var up = 0

  override protected def _advance: Option[(Pos, Call)] =
    if (up >= uncompressedSize)
      None
    else {
      val pos = Pos(block, up)
      Some(
        pos →
          checker(pos)
      )
    }

  override protected def postNext(): Unit = {
    up += 1
  }
}
