package org.hammerlab.bgzf.block

import java.io.{ IOException, InputStream }
import java.nio.ByteBuffer

import org.hammerlab.io.ByteChannel

/**
 * BGZF-block header
 * @param size size of header, in bytes
 * @param compressedSize compressed size of block, parsed from header
 */
case class Header(size: Int, compressedSize: Int)

object Header {

  // 18 bytes is enough to learn what we need to know: sizes of header and compressed block
  val EXPECTED_HEADER_SIZE = 18

  def apply(ch: ByteChannel)(implicit buf: ByteBuffer): Header = {
    buf.limit(EXPECTED_HEADER_SIZE)
    ch.read(buf)

    implicit val arr = buf.array
    val header = apply()
    buf.clear()
    ch.skip(header.size - EXPECTED_HEADER_SIZE)

    header
  }

  def apply(is: InputStream)(implicit buffer: Array[Byte]): Header = {

    val headerBytesRead = is.read(buffer, 0, EXPECTED_HEADER_SIZE)
    if (headerBytesRead != EXPECTED_HEADER_SIZE)
      throw new IOException(
        s"Expected $EXPECTED_HEADER_SIZE header bytes, got $headerBytesRead"
      )

    val header = apply()
    is.skip(header.size - EXPECTED_HEADER_SIZE)

    header
  }

  def apply(offset: Int = 0)(implicit bytes: Array[Byte]): Header = make(offset, bytes.length)
  def make(offset: Int, length: Int)(implicit bytes: Array[Byte]): Header = {

    def check(idx: Int, expected: Byte): Unit = {
      val actual = bytes(idx)
      if (actual != expected)
        throw HeaderParseException(
          idx,
          actual,
          expected
        )
    }

    // GZip magic bytes
    check(0,  31)
    check(1, 139.toByte)
    check(2,   8)
    check(3,   4)

    val xlen = getShort(10)

    // We expect 6 bytes of `xlen`; anything more is considered "extra" and added to the expected 18-byte header size
    val extraHeaderBytes = xlen - 6
    val actualHeaderSize = EXPECTED_HEADER_SIZE + extraHeaderBytes

    // BAM-specific GZip-flags
    check(12, 66)
    check(13, 67)
    check(14,  2)

    val compressedSize = getShort(16) + 1

    Header(
      actualHeaderSize,
      compressedSize
    )
  }

  def getShort(idx: Int)(implicit buffer: Array[Byte]): Int =
    (buffer(idx) & 0xff) |
      ((buffer(idx + 1) & 0xff) << 8)
}
