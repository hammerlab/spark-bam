package org.hammerlab.hadoop_bam.bam

import java.io.InputStream

import htsjdk.samtools.{ BAMRecordCodec, SAMRecord }
import org.hammerlab.hadoop_bam.bgzf.Pos
import org.hammerlab.paths.Path
import org.seqdoop.hadoop_bam.LazyBAMRecordFactory
import sun.nio.ch.ChannelInputStream

case class RecordStream(override val path: Path)
  extends RecordIterator[(Pos, SAMRecord)](path) {

  val byteStream: InputStream = new ChannelInputStream(byteChannel)

  val bamCodec = new BAMRecordCodec(null, new LazyBAMRecordFactory)
  bamCodec.setInputStream(byteStream)

  override protected def _advance: Option[(Pos, SAMRecord)] = {
    for {
      pos ← curPos
      rec ← Option(bamCodec.decode())
    } yield
      pos → rec
  }
}

class SeekableRecordStream(override val path: Path)
  extends RecordStream(path)
    with SeekableRecordIterator[(Pos, SAMRecord)]

object SeekableRecordStream {
  def apply(path: Path): SeekableRecordStream = new SeekableRecordStream(path)
}
