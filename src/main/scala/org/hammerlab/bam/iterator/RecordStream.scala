package org.hammerlab.bam.iterator

import java.io.InputStream
import java.nio.channels.{ FileChannel, SeekableByteChannel }

import htsjdk.samtools.{ BAMRecordCodec, SAMRecord }
import org.hammerlab.bgzf.Pos
import org.hammerlab.paths.Path
import org.seqdoop.hadoop_bam.LazyBAMRecordFactory
import sun.nio.ch.ChannelInputStream

trait RecordStreamI
  extends RecordIterator[(Pos, SAMRecord)] {

  lazy val byteStream: InputStream = new ChannelInputStream(byteChannel)

  lazy val bamCodec = {
    val codec = new BAMRecordCodec(null, new LazyBAMRecordFactory)
    codec.setInputStream(byteStream)
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

case class RecordStream(compressedInputStream: InputStream)
  extends RecordStreamI

object RecordStream {
  def apply(path: Path): RecordStream = RecordStream(path.inputStream)
}

case class SeekableRecordStream(compressedChannel: SeekableByteChannel)
  extends RecordStreamI
    with SeekableRecordIterator[(Pos, SAMRecord)]

object SeekableRecordStream {
  def apply(path: Path): SeekableRecordStream = SeekableRecordStream(FileChannel.open(path))
}
