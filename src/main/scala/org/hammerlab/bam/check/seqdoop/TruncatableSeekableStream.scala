package org.hammerlab.bam.check.seqdoop

import htsjdk.samtools.seekablestream.SeekableStream
import org.hammerlab.paths.Path
import org.hammerlab.io.SeekableByteChannel

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

  override def read(): Int =
    if (position() < length())
      channel.read()
    else
      -1

  override def read(b: Array[Byte],
                    off: Int,
                    len: Int): Int = {
    channel.read(b, off, min(len, length() - off).toInt)
  }

  override def close(): Unit = channel.close()
}
