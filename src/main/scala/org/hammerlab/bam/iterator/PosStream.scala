package org.hammerlab.bam.iterator

import org.hammerlab.bam.header.Header
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ SeekableUncompressedBytes, UncompressedBytes, UncompressedBytesI }
import org.hammerlab.io.{ ByteChannel, SeekableByteChannel }

/**
 * Interface for iterating over record-start [[Pos]]s in a BAM file
 */
trait PosStreamI[Stream <: UncompressedBytesI[_]]
  extends RecordIterator[Pos, Stream] {

  override protected def _advance: Option[Pos] = {
    for {
      pos ← curPos
    } yield {
      val remainingLength = uncompressedByteChannel.getInt
      uncompressedBytes.drop(remainingLength)
      pos
    }
  }
}

/**
 * Non-seekable [[PosStreamI]]
 */
case class PosStream(uncompressedBytes: UncompressedBytes,
                     header: Header)
  extends PosStreamI[UncompressedBytes]

object PosStream {
  def apply(ch: ByteChannel): PosStream = {
    val uncompressedBytes = UncompressedBytes(ch)
    val header = Header(uncompressedBytes)
    PosStream(
      uncompressedBytes,
      header
    )
  }
}

/**
 * Seekable [[PosStreamI]]
 */
case class SeekablePosStream(uncompressedBytes: SeekableUncompressedBytes,
                             header: Header)
  extends PosStreamI[SeekableUncompressedBytes]
    with SeekableRecordIterator[Pos]

object SeekablePosStream {
  def apply(ch: SeekableByteChannel): SeekablePosStream = {
    val uncompressedBytes = SeekableUncompressedBytes(ch)
    val header = Header(uncompressedBytes)
    SeekablePosStream(
      uncompressedBytes,
      header
    )
  }
}
