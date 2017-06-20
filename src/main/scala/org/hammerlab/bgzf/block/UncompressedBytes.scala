package org.hammerlab.bgzf.block

import java.io.Closeable

import org.hammerlab.bgzf.Pos
import org.hammerlab.io.{ ByteChannel, SeekableByteChannel }
import org.hammerlab.iterator.FlatteningIterator._
import org.hammerlab.iterator.SimpleBufferedIterator

/**
 * [[Iterator]] of bgzf-decompressed bytes from a [[Stream]] of [[Block]]s.
 * @tparam BlockStream underlying [[Block]]-[[Stream]] type (basically: seekable or not?).
 */
trait UncompressedBytesI[BlockStream <: StreamI]
  extends SimpleBufferedIterator[Byte]
    with Closeable {
  def blockStream: BlockStream
  val uncompressedBytes = blockStream.smush
  def curBlock: Option[Block] = uncompressedBytes.cur
  def curPos: Option[Pos] = curBlock.map(_.pos)

  override protected def _advance: Option[Byte] = uncompressedBytes.nextOption

  override def close(): Unit = {
    blockStream.close()
  }
}

/**
 * Non-seekable [[UncompressedBytesI]]
 */
case class UncompressedBytes(blockStream: Stream)
  extends UncompressedBytesI[Stream]

object UncompressedBytes {
  def apply(compressedBytes: ByteChannel): UncompressedBytes = UncompressedBytes(Stream(compressedBytes))
}

/**
 * Seekable [[UncompressedBytesI]]
 */
case class SeekableUncompressedBytes(blockStream: SeekableStream)
  extends UncompressedBytesI[SeekableStream] {
  def seek(pos: Pos): Unit = {
    blockStream.seek(pos.blockPos)
    uncompressedBytes.reset()
    clear()
    curBlock
      .foreach {
        block ⇒
          block.idx = pos.offset
      }

  }
}

object SeekableUncompressedBytes {
  def apply(ch: SeekableByteChannel): SeekableUncompressedBytes =
    SeekableUncompressedBytes(
      SeekableStream(
        ch
      )
    )
}
