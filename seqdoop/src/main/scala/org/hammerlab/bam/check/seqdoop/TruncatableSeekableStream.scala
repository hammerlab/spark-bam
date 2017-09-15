package org.hammerlab.bam.check.seqdoop

import java.io.EOFException

import htsjdk.samtools.seekablestream.SeekableStream
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.paths.Path

import scala.math.min

case class TruncatableSeekableStream(channel: SeekableByteChannel,
                                     source: Path)
  extends SeekableStream {

  var limit = channel.size

  def clear(): Unit =
    limit = channel.size

  override def length(): Long = limit

  override def seek(position: Long): Unit =
    channel.seek(
      min(
        limit,
        position
      )
    )

  override def getSource: String = source.toString()

  override def position(): Long = channel.position()
  override def eof(): Boolean = channel.position() == length()

  def remaining: Long = length() - position()

  override def read(): Int =
    if (position() < length())
      channel.read()
    else
      -1

  override def read(b: Array[Byte],
                    off: Int,
                    len: Int): Int = {
    if (len > remaining) {
      channel.read(b, off, remaining.toInt)
      throw new EOFException(
        s"Attempting to read $len bytes from offset $off when channel is at ${position()} with length ${length()} (only $remaining bytes available)"
      )
    }
    channel.read(b, off, len)
  }

  override def close(): Unit = channel.close()
}
