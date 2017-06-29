package org.hammerlab.bgzf.block

import java.io.{ Closeable, EOFException, IOException }

import org.hammerlab.bgzf.block.Block.FOOTER_SIZE
import org.hammerlab.bgzf.block.Header.EXPECTED_HEADER_SIZE
import org.hammerlab.io.{ Buffer, ByteChannel }
import org.hammerlab.iterator.SimpleBufferedIterator

/**
 * Iterator over bgzf-block [[Metadata]]; useful when loading/decompressing [[Block]] payloads is unnecessary.
 *
 * @param ch input stream/channel containing compressed bgzf data
 * @param includeEmptyFinalBlock if true, include the final, empty bgzf-block in this stream
 */
case class MetadataStream(ch: ByteChannel,
                          includeEmptyFinalBlock: Boolean = false)
  extends SimpleBufferedIterator[Metadata]
    with Closeable {

  // Buffer for the standard bits of the header that we care about
  implicit val buf = Buffer(EXPECTED_HEADER_SIZE)

  override protected def _advance: Option[Metadata] = {

    val start = ch.position()

    buf.clear()
    val Header(actualHeaderSize, compressedSize) =
      try {
        Header(ch)
      } catch {
        case e: EOFException ⇒
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
          compressedSize,
          uncompressedSize
        )
      )
  }

  override def close(): Unit =
    ch.close()
}
