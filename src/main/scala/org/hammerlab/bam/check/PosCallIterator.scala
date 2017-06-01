package org.hammerlab.bam.check

import org.hammerlab.bam.check.Error.Flags
import org.hammerlab.bam.iterator.SeekableRecordStream
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableByteStream
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.io.SeekableByteChannel
import org.hammerlab.iterator.SimpleBufferedIterator

case class PosCallIterator(block: Long,
                           usize: Int,
                           ch: SeekableByteStream,
                           contigLengths: Map[Int, NumLoci])
  extends SimpleBufferedIterator[(Pos, Option[Flags])] {

  val rs = new SeekableRecordStream(ch)
  val finder = new RecordFinder

  var up = 0
  override protected def _advance: Option[(Pos, Option[Flags])] =
    if (up >= usize)
      None
    else {
      val pos = Pos(block, up)
      rs.seek(pos)

      Some(
        pos →
          finder(
            rs.uncompressedByteChannel,
            contigLengths
          )
      )
    }

  override protected def postNext(): Unit = {
    up += 1
  }

  override def close(): Unit = ch.close()
}
