package org.hammerlab.bam.iterator

import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ SeekableByteStream, SeekableStream }
import org.hammerlab.io.SeekableByteChannel

/**
 * Interface for [[RecordIterator]]s that adds ability to seek
 */
trait SeekableRecordIterator[T]
  extends RecordIterator[T, SeekableByteStream] {

  //override def makeBlockStream: SeekableStream = SeekableStream(compressedByteChannel)

  def seek(to: Pos): Unit = {
    if (to < headerEndPos) {
      // Positions inside the header should fast-forward to the end of the header
      seek(headerEndPos)
    } else {
      stream.seek(to)

      /** Clear any cached [[org.hammerlab.iterator.SimpleBufferedIterator]] `_next` values */
      clear()

//      uncompressedBytes.clear()

      // Fast-forward to requested virtual offset within block
//      blockStream.headOption.foreach(
//        block ⇒
//          block.idx = to.offset
//      )

//      println("record seek done")
      assert(curPos.get == to)
//      println("record seek assertion done")
    }
  }
}
