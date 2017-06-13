package org.hammerlab.bgzf.block

import org.hammerlab.bgzf.Pos
import org.hammerlab.io.{ ByteChannel, SeekableByteChannel }
import org.hammerlab.iterator.FlatteningIterator._
import org.hammerlab.iterator.SimpleBufferedIterator

/**
 * [[Iterator]] of bgzf-decompressed bytes from a [[Stream]] of [[Block]]s.
 * @tparam S underlying [[Block]]-[[Stream]] type (basically: seekable or not?).
 */
trait ByteStreamI[S <: StreamI]
  extends SimpleBufferedIterator[Byte] {
  def stream: S
  val it = stream.smush
  def curBlock: Option[Block] = it.cur
  def curPos: Option[Pos] = curBlock.map(_.pos)

  override protected def _advance: Option[Byte] = it.nextOption

  override def close(): Unit = {
    super.close()
    stream.close()
  }
}

/**
 * Non-seekable [[ByteStreamI]]
 */
case class ByteStream(stream: Stream)
  extends ByteStreamI[Stream]

object ByteStream {
  def apply(compressedBytes: ByteChannel): ByteStream = ByteStream(Stream(compressedBytes))
}

/**
 * Seekable [[ByteStreamI]]
 */
case class SeekableByteStream(stream: SeekableStream)
  extends ByteStreamI[SeekableStream] {
  def seek(pos: Pos): Unit = {
    stream.seek(pos.blockPos)
    it.reset()
    clear()
    curBlock
      .foreach {
        block ⇒
          block.idx = pos.offset
      }

  }
}

object SeekableByteStream {
  def apply(ch: SeekableByteChannel): SeekableByteStream =
    SeekableByteStream(
      SeekableStream(
        ch
      )
    )
}
