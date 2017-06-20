package org.hammerlab.bam.iterator

import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes

/**
 * Interface for [[RecordIterator]]s that adds ability to seek
 */
trait SeekableRecordIterator[T]
  extends RecordIterator[T, SeekableUncompressedBytes] {

  def seek(to: Pos): Unit = {
    if (to < headerEndPos) {
      // Positions inside the header should fast-forward to the end of the header
      seek(headerEndPos)
    } else {
      uncompressedBytes.seek(to)

      /** Clear any cached [[org.hammerlab.iterator.SimpleBufferedIterator]] `_next` values */
      clear()

      assert(curPos.get == to)
    }
  }
}
