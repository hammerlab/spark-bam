package org.hammerlab.bam.iterator

import java.nio.channels.FileChannel

import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ ByteStream, ByteStreamI, SeekableByteStream }
import org.hammerlab.paths.Path

/**
 * Interface for iterating over record-start [[Pos]]s in a BAM file
 */
trait PosStreamI[Stream <: ByteStreamI[_]]
  extends RecordIterator[Pos, Stream] {
  override protected def _advance: Option[Pos] = {
    for {
      pos ← curPos
    } yield {
      val remainingLength = readInt
      stream.drop(remainingLength)
      pos
    }
  }
}

/**
 * Non-seekable [[PosStreamI]]
 */
case class PosStream(stream: ByteStream)
  extends PosStreamI[ByteStream]

object PosStream {
  def apply(path: Path): PosStream =
    PosStream(
      ByteStream(
        path.inputStream
      )
    )
}

/**
 * Seekable [[PosStreamI]]
 */
case class SeekablePosStream(stream: SeekableByteStream)
  extends PosStreamI[SeekableByteStream]
    with SeekableRecordIterator[Pos]

object SeekablePosStream {
  def apply(path: Path): SeekablePosStream =
    SeekablePosStream(
      SeekableByteStream(
        FileChannel.open(path)
      )
    )
}
