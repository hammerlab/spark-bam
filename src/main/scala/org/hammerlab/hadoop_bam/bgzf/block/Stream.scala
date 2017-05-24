package org.hammerlab.hadoop_bam.bgzf.block

import java.io.{ IOException, InputStream }
import java.util.zip.Inflater

import org.hammerlab.hadoop_bam.bgzf.block.Block.{ FOOTER_SIZE, MAX_BLOCK_SIZE, getInt }
import org.hammerlab.iterator.SimpleBufferedIterator

/**
 * Iterator over BGZF [[Block]]s pointed to by a BGZF-compressed [[InputStream]]
 */
case class Stream(is: InputStream)
  extends SimpleBufferedIterator[Block] {

  implicit val encBuf = Array.fill[Byte](MAX_BLOCK_SIZE)(0)
  val decBuf = Array.fill[Byte](MAX_BLOCK_SIZE)(0)

  var blockStart = 0L
  def pos = head.pos

  var blockIdx = -1

  override protected def _advance: Option[Block] = {

    blockIdx += 1

    val Header(actualHeaderSize, compressedSize) = Header(is)

    val dataLength = compressedSize - actualHeaderSize - FOOTER_SIZE

    val remainingBytes = dataLength + FOOTER_SIZE

    val bytesRead = is.read(encBuf, actualHeaderSize, remainingBytes)
    if (bytesRead != remainingBytes) {
      throw new IOException(s"Expected $remainingBytes bytes for block data+footer, found $bytesRead")
    }

    val uncompressedSize = getInt(compressedSize - 4)

    val inflater = new Inflater(true)
    inflater.setInput(encBuf, actualHeaderSize, dataLength)
    val bytesDecompressed = inflater.inflate(decBuf, 0, uncompressedSize)
    if (bytesDecompressed != uncompressedSize) {
      throw new IOException(s"Expected $uncompressedSize decompressed bytes, found $bytesDecompressed")
    }

    val start = blockStart
    blockStart += compressedSize

    if (dataLength == 2)
      // Empty block at end of file
      None
    else
      Some(
        Block(
          decBuf.slice(0, uncompressedSize),
          start,
          compressedSize
        )
      )
  }
}
