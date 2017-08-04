package org.hammerlab.bam.iterator

import htsjdk.samtools.{ BAMRecordCodec, DefaultSAMRecordFactory, SAMRecord }
import org.hammerlab.bam.header.Header
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ SeekableUncompressedBytes, UncompressedBytes, UncompressedBytesI }
import org.hammerlab.channel.{ ByteChannel, SeekableByteChannel }

/**
 * Interface for iterating over BAM records (keyed by [[Pos]])
 */
trait RecordStreamI[Stream <: UncompressedBytesI[_]]
  extends RecordIterator[(Pos, SAMRecord), Stream] {

  lazy val bamCodec = {
    val codec =
      new BAMRecordCodec(
        header,
        DefaultSAMRecordFactory.getInstance()
      )

    codec.setInputStream(uncompressedByteChannel)
    codec
  }

  override protected def _advance: Option[(Pos, SAMRecord)] = {
    for {
      pos ← curPos
      rec ← Option(bamCodec.decode())
    } yield
      pos → rec
  }
}

/**
 * Non-seekable [[RecordStreamI]]
 */
case class RecordStream[Stream <: UncompressedBytesI[_]](uncompressedBytes: Stream,
                                                         header: Header)
  extends RecordStreamI[Stream]

object RecordStream {
  implicit def apply(ch: ByteChannel): RecordStream[UncompressedBytes] = {
    val uncompressedBytes = UncompressedBytes(ch)
    RecordStream(
      uncompressedBytes,
      Header(uncompressedBytes)
    )
  }
}

/**
 * Seekable [[RecordStreamI]]
 */
case class SeekableRecordStream(uncompressedBytes: SeekableUncompressedBytes,
                                header: Header)
  extends RecordStreamI[SeekableUncompressedBytes]
    with SeekableRecordIterator[(Pos, SAMRecord)]


object SeekableRecordStream {
  def apply(ch: SeekableByteChannel): SeekableRecordStream = {
    val uncompressedBytes = SeekableUncompressedBytes(ch)
    SeekableRecordStream(
      uncompressedBytes,
      Header(uncompressedBytes)
    )
  }
}
