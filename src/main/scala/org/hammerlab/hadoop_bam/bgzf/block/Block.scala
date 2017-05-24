package org.hammerlab.hadoop_bam.bgzf.block

import org.hammerlab.hadoop_bam.bgzf.Pos

case class Block(bytes: Array[Byte],
                 start: Long,
                 compressedSize: Int)
  extends Iterator[Byte] {
  def uncompressedSize = bytes.length
  def startPos = Pos(start, 0)
  def endPos = Pos(start, uncompressedSize)
  def nextStartPos = Pos(start + compressedSize, 0)

  def pos = Pos(start, idx)

  var idx = 0
  override def hasNext: Boolean = idx < uncompressedSize
  override def next(): Byte = {
    val ret = bytes(idx)
    idx += 1
    ret
  }

  override def toString(): String = {
    s"Block($startPos-$endPos;$compressedSize)"
  }
}

object Block {
  val MAX_BLOCK_SIZE = 64 * 1024

  val FOOTER_SIZE =  8  // CRC32 (4), uncompressed size (4)

  def getInt(idx: Int)(implicit buffer: Array[Byte]): Int =
    (buffer(idx) & 0xff) |
      ((buffer(idx + 1) & 0xff) << 8) |
      ((buffer(idx + 2) & 0xff) << 16) |
      ((buffer(idx + 3) & 0xff) << 24)
}

case class HeaderParseException(idx: Int,
                                actual: Byte,
                                expected: Byte)
  extends Exception(
    s"Position $idx: $actual != $expected"
  )
