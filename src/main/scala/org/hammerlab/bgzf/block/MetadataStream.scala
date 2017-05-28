package org.hammerlab.bgzf.block

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder.LITTLE_ENDIAN

import org.hammerlab.bgzf.block.Block.{ FOOTER_SIZE, MAX_BLOCK_SIZE }
import org.hammerlab.io.ByteChannel
import org.hammerlab.iterator.SimpleBufferedIterator

case class MetadataStream(ch: ByteChannel)
  extends SimpleBufferedIterator[Metadata] {

  implicit val encBuf =
    ByteBuffer
      .allocate(MAX_BLOCK_SIZE)
      .order(LITTLE_ENDIAN)

  var blockStart = 0L
  def pos = head.start

  var blockIdx = -1

  override protected def _advance: Option[Metadata] = {

    blockIdx += 1

    encBuf.clear()
    val Header(actualHeaderSize, compressedSize) =
      try {
        Header(ch)
      } catch {
        case e: IOException ⇒
          return None
      }

    val dataLength = compressedSize - actualHeaderSize - FOOTER_SIZE

    val remainingBytes = dataLength + FOOTER_SIZE

    ch.skip(remainingBytes - 4)
    encBuf.limit(4)
    val bytesRead = ch.read(encBuf)
    if (bytesRead != 4)
      throw new IOException(
        s"Expected 4 bytes for block data+footer, found $bytesRead"
      )

    encBuf.position(0)
    val uncompressedSize = encBuf.getInt

    val start = blockStart
    blockStart += compressedSize

    if (dataLength == 2)
      // Empty block at end of file
      None
    else
      Some(
        Metadata(
          start,
          uncompressedSize,
          compressedSize
        )
      )
  }
}
