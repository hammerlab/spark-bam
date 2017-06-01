package org.hammerlab.bgzf.block

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder.LITTLE_ENDIAN

import org.hammerlab.bgzf.block.Block.{ FOOTER_SIZE, MAX_BLOCK_SIZE }
import org.hammerlab.io.ByteChannel
import org.hammerlab.iterator.SimpleBufferedIterator

/**
 * Iterator over bgzf-block [[Metadata]]
 * @param ch input stream/channel containing compressed bgzf data
 * @param includeEmptyFinalBlock if true, include the final, empty bgzf-block in this stream
 */
case class MetadataStream(ch: ByteChannel,
                          includeEmptyFinalBlock: Boolean = false,
                          closeStream: Boolean = true)
  extends SimpleBufferedIterator[Metadata] {

  // Buffer for compressed-block data
  implicit val encBuf =
    ByteBuffer
      .allocate(MAX_BLOCK_SIZE)
      .order(LITTLE_ENDIAN)

  override protected def _advance: Option[Metadata] = {

    val start = ch.position()

    encBuf.clear()
    val Header(actualHeaderSize, compressedSize) =
      try {
        Header(ch)
      } catch {
        case e: IOException ⇒
          return None
      }

    val remainingBytes = compressedSize - actualHeaderSize

    ch.skip(remainingBytes - 4)
    val uncompressedSize = ch.getInt

    val dataLength = remainingBytes - FOOTER_SIZE

    if (dataLength == 2 && !includeEmptyFinalBlock) {
      // Skip empty block at end of file
      None
    } else
      Some(
        Metadata(
          start,
          uncompressedSize,
          compressedSize
        )
      )
  }

  override def close(): Unit =
    if (closeStream)
      ch.close()
}
