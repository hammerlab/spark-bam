package org.hammerlab.bam.iterator

import java.nio.channels.FileChannel

import htsjdk.samtools.{ BAMRecordCodec, SAMRecord }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ ByteStream, ByteStreamI, SeekableByteStream }
import org.hammerlab.paths.Path
import org.seqdoop.hadoop_bam.LazyBAMRecordFactory

/**
 * Interface for iterating over BAM records (keyed by [[Pos]])
 */
trait RecordStreamI[Stream <: ByteStreamI[_]]
  extends RecordIterator[(Pos, SAMRecord), Stream] {

  lazy val bamCodec = {
    val codec = new BAMRecordCodec(null, new LazyBAMRecordFactory)
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
case class RecordStream(stream: ByteStream)
  extends RecordStreamI[ByteStream]

object RecordStream {
  def apply(path: Path): RecordStream =
    RecordStream(
      ByteStream(
        path.inputStream
      )
    )
}

/**
 * Seekable [[RecordStreamI]]
 */
case class SeekableRecordStream(stream: SeekableByteStream)
  extends RecordStreamI[SeekableByteStream]
    with SeekableRecordIterator[(Pos, SAMRecord)]

object SeekableRecordStream {
  def apply(path: Path): SeekableRecordStream =
    SeekableRecordStream(
      SeekableByteStream(
        FileChannel.open(path)
      )
    )
}