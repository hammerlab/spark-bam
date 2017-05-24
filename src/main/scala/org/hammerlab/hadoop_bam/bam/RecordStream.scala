package org.hammerlab.hadoop_bam.bam

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder.LITTLE_ENDIAN
import java.nio.channels.{ FileChannel, ReadableByteChannel }

import htsjdk.samtools.{ BAMRecordCodec, SAMRecord }
import org.apache.hadoop.conf.Configuration
import org.hammerlab.hadoop_bam.bgzf.{ Pos, block }
import org.hammerlab.hadoop_bam.bgzf.block.Block
import org.hammerlab.iterator.FlatteningIterator._
import org.hammerlab.iterator.{ FlatteningIterator, SimpleBufferedIterator }
import org.hammerlab.paths.Path
import org.seqdoop.hadoop_bam.LazyBAMRecordFactory
import sun.nio.ch.ChannelInputStream

case class RecordStream(path: Path, conf: Configuration)
  extends SimpleBufferedIterator[(Pos, SAMRecord)] {

  val compressedChannel = FileChannel.open(path)
  val compressedInputStream = new ChannelInputStream(compressedChannel)

  val blockStream: block.Stream = block.Stream(compressedInputStream)
  val uncompressedBytes: FlatteningIterator[Byte, Block] = blockStream.smush

  val byteChannel: ByteChannel = ByteChannel(uncompressedBytes)
  val byteStream: InputStream = new ChannelInputStream(byteChannel)

  val bamCodec = new BAMRecordCodec(null, new LazyBAMRecordFactory)
  bamCodec.setInputStream(byteStream)

  def getHeaderEndPos: Pos = {
    val b4 = ByteBuffer.allocate(4).order(LITTLE_ENDIAN)
    byteChannel.read(b4)
    require(b4.array().map(_.toChar).mkString("") == "BAM\1")

    def readInt: Int = {
      b4.position(0)
      byteChannel.read(b4)
      b4.position(0)
      b4.getInt
    }

    val headerLength = readInt
    println(s"header length: $headerLength (${b4.position()})")

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
      println(s"ref: ${nameBuffer.array().dropRight(1).map(_.toChar).mkString("")}: $refLen")
    }

    blockStream.pos
  }

  val headerEndPos = getHeaderEndPos
  println(s"header end: $headerEndPos")

  def curBlock: Option[Block] = uncompressedBytes.cur
  def curPos: Option[Pos] = curBlock.map(_.pos)

  def seek(to: Pos): Unit = {
    if (to == Pos(0, 0)) {
      seek(headerEndPos)
    } else {
      compressedChannel.position(to.blockPos)
      clear()
      blockStream.clear()
      blockStream.blockStart = to.blockPos
      uncompressedBytes.clear()
      uncompressedBytes.drop(to.offset)
      assert(blockStream.pos.blockPos == to.blockPos, s"Expected ${blockStream.pos} to match $to")
    }
  }

  override protected def _advance: Option[(Pos, SAMRecord)] = {
    for {
      pos ← curPos
      rec ← Option(bamCodec.decode())
    } yield
      pos → rec
  }
}

case class ByteStream(it: Iterator[Byte]) extends InputStream {
  override def read(): Int = it.next
}

case class ByteChannel(it: Iterator[Byte])
  extends ReadableByteChannel {
  override def read(dst: ByteBuffer): Int = {
    var idx = 0
    val size = dst.limit() - dst.position()
    while (idx < size && it.hasNext) {
      dst.put(it.next)
      idx += 1
    }
    if (idx == 0 && !it.hasNext)
      -1
    else
      idx
  }
  override def isOpen: Boolean = true
  override def close(): Unit = {}
}
