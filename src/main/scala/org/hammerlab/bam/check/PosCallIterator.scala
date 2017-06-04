package org.hammerlab.bam.check

import org.hammerlab.bgzf.Pos
import org.hammerlab.iterator.SimpleBufferedIterator

case class PosCallIterator[Call](block: Long,
                                 usize: Int,
                                 checker: Checker[Call])
  extends SimpleBufferedIterator[(Pos, Call)] {

  var up = 0

  override protected def _advance: Option[(Pos, Call)] =
    if (up >= usize)
      None
    else {
      val pos = Pos(block, up)

      checker.seek(pos)

      Some(
        pos →
          checker()
      )
    }

  override protected def postNext(): Unit = {
    up += 1
  }
}
