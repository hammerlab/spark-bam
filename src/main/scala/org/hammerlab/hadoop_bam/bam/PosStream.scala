package org.hammerlab.hadoop_bam.bam

import org.hammerlab.hadoop_bam.bgzf.Pos
import org.hammerlab.paths.Path

case class PosStream(override val path: Path)
  extends RecordIterator[Pos](path) {
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

class SeekablePosStream(override val path: Path)
  extends PosStream(path)
    with SeekableRecordIterator[Pos]

object SeekablePosStream {
  def apply(path: Path): SeekablePosStream = new SeekablePosStream(path)
}
