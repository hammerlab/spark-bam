package org.hammerlab.bam.iterator

import java.io.InputStream
import java.nio.channels.{ FileChannel, SeekableByteChannel }

import org.hammerlab.bgzf.Pos
import org.hammerlab.paths.Path

trait PosStreamI
  extends RecordIterator[Pos] {
  override protected def _advance: Option[Pos] = {
    for {
      pos ← curPos
    } yield {
      val remainingLength = readInt
      uncompressedBytes.drop(remainingLength)
      pos
    }
  }
}

case class PosStream(compressedInputStream: InputStream)
  extends PosStreamI

object PosStream {
  def apply(path: Path): PosStream = PosStream(path.inputStream)
}

case class SeekablePosStream(compressedChannel: SeekableByteChannel)
  extends PosStreamI
    with SeekableRecordIterator[Pos]

object SeekablePosStream {
  def apply(path: Path): SeekablePosStream = SeekablePosStream(FileChannel.open(path))
}
