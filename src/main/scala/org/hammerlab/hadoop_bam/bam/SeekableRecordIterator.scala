package org.hammerlab.hadoop_bam.bam

import java.nio.channels.SeekableByteChannel

import org.hammerlab.hadoop_bam.bgzf.Pos
import sun.nio.ch.ChannelInputStream

trait SeekableRecordIterator[T] {
  self: RecordIterator[T] ⇒

  def compressedChannel: SeekableByteChannel

  override lazy val compressedInputStream = new ChannelInputStream(compressedChannel)

  def seek(to: Pos): Unit = {
    if (to < headerEndPos) {
      seek(headerEndPos)
    } else {
      compressedChannel.position(to.blockPos)
      reset()
      blockStream.blockStart = to.blockPos
      uncompressedBytes.drop(to.offset)
      assert(blockStream.pos.blockPos == to.blockPos, s"Expected ${blockStream.pos} to match $to")
    }
  }
}
