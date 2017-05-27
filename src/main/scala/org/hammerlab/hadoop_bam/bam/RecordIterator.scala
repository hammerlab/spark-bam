package org.hammerlab.hadoop_bam.bam

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder.LITTLE_ENDIAN

import org.hammerlab.hadoop_bam.bgzf.block.Block
import org.hammerlab.hadoop_bam.bgzf.{ Pos, block }
import org.hammerlab.iterator.FlatteningIterator._
import org.hammerlab.iterator.{ FlatteningIterator, SimpleBufferedIterator }

trait RecordIterator[T]
  extends SimpleBufferedIterator[T] {

  def compressedInputStream: InputStream

  val blockStream: block.Stream = block.Stream(compressedInputStream)
  val uncompressedBytes: FlatteningIterator[Byte, Block] = blockStream.smush

  val byteChannel: ByteChannel = uncompressedBytes

  val b4 = ByteBuffer.allocate(4).order(LITTLE_ENDIAN)

  def readInt: Int = {
    b4.position(0)
    byteChannel.read(b4)
    b4.position(0)
    b4.getInt
  }

  def getHeaderEndPos: Pos = {
    byteChannel.read(b4)
    require(b4.array().map(_.toChar).mkString("") == "BAM\1")

    val headerLength = readInt

    /** Skip [[headerLength]] bytes */
    uncompressedBytes.drop(headerLength)

    val numReferences = readInt

    for {
      refIdx ← 0 until numReferences
    } {
      val refNameLen = readInt
      val nameBuffer = ByteBuffer.allocate(refNameLen)
      byteChannel.read(nameBuffer)

      val refLen = readInt
    }

    blockStream.pos
  }

  def reset(): Unit = {
    super.clear()
    blockStream.clear()
    uncompressedBytes.clear()
  }

  val headerEndPos = getHeaderEndPos

  def curBlock: Option[Block] = uncompressedBytes.cur
  def curPos: Option[Pos] = curBlock.map(_.pos)
}
