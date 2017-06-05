package org.hammerlab.bam.iterator

import java.io.InputStream

import org.hammerlab.bam.Header
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ Block, ByteStreamI }
import org.hammerlab.io.{ Buffer, ByteChannel }
import org.hammerlab.iterator.SimpleBufferedIterator

/**
 * Interface for iterators that wrap a (compressed) BAM-file [[InputStream]] and emit one object for each underlying
 * record.
 */
trait RecordIterator[T, Stream <: ByteStreamI[_]]
  extends SimpleBufferedIterator[T] {

  // Uncompressed bytes; also exposes pointer to current-block
  val stream: Stream

  // Uncompressed byte-channel, for reading ints into a buffer
  val uncompressedByteChannel: ByteChannel = stream

  val Header(_, headerEndPos, _) = Header(stream)

  def curBlock: Option[Block] = stream.curBlock
  def curPos: Option[Pos] = stream.curPos

  override def close(): Unit =
    stream.close()
}
