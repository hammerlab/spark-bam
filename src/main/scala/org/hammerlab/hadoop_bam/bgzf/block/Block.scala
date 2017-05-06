package org.hammerlab.hadoop_bam.bgzf.block

import java.io.{ IOException, InputStream }

import org.hammerlab.hadoop_bam.bgzf.VirtualPos

case class Block(bytes: Array[Byte],
                 start: Long,
                 compressedSize: Int) {
  def uncompressedSize = bytes.length
  def startPos = VirtualPos(start, 0)
  def endPos = VirtualPos(start, uncompressedSize)
}

case class Header(size: Int, compressedSize: Int)

object Block {
  val MAX_BLOCK_SIZE = 64 * 1024

  val EXPECTED_HEADER_SIZE = 18
  val FOOTER_SIZE =  8  // CRC32 (4), uncompressed size (4)

  def getShort(idx: Int)(implicit buffer: Array[Byte]): Int =
    (buffer(idx) & 0xff) |
      ((buffer(idx + 1) & 0xff) << 8)

  def getInt(idx: Int)(implicit buffer: Array[Byte]): Int =
    (buffer(idx) & 0xff) |
      ((buffer(idx + 1) & 0xff) << 8) |
      ((buffer(idx + 2) & 0xff) << 16) |
      ((buffer(idx + 3) & 0xff) << 24)

  def readHeader(is: InputStream)(implicit buffer: Array[Byte]): Header = {

    val headerBytesRead = is.read(buffer, 0, EXPECTED_HEADER_SIZE)
    if (headerBytesRead != EXPECTED_HEADER_SIZE) {
      throw new IOException(s"Expected $EXPECTED_HEADER_SIZE header bytes, got $headerBytesRead")
    }

    val header = readHeader()
    is.skip(header.size - EXPECTED_HEADER_SIZE)

    header
  }

  def readHeader(offset: Int = 0)(implicit bytes: Array[Byte]): Header = readHeader(offset, bytes.length)
  def readHeader(offset: Int, length: Int)(implicit bytes: Array[Byte]): Header = {

    def check(idx: Int, expected: Byte): Unit = {
      val actual = bytes(idx)
      if (actual != expected)
        throw HeaderParseException(
          idx,
          actual,
          expected
        )
    }

    check(0,  31)
    check(1, 139.toByte)
    check(2,   8)
    check(3,   4)

    val xlen = getShort(10)

    val extraHeaderBytes = xlen - 6
    val actualHeaderSize = EXPECTED_HEADER_SIZE + extraHeaderBytes

    check(12, 66)
    check(13, 67)
    check(14,  2)

    val compressedSize = getShort(16) + 1

    Header(
      actualHeaderSize,
      compressedSize
    )
  }
}

case class HeaderParseException(idx: Int,
                                actual: Byte,
                                expected: Byte)
  extends Exception(
    s"Position $idx: $actual != $expected"
  )
