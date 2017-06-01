package org.hammerlab.io

import java.io.{ IOException, InputStream }
import java.nio.channels.ReadableByteChannel
import java.nio.{ ByteBuffer, ByteOrder, channels }

import org.apache.hadoop.fs.Seekable

/**
 * Readable, "skippable" interface over [[InputStream]]s, [[Iterator[Byte]]]s, and [[channels.SeekableByteChannel]]s.
 *
 * When wrapping [[channels.SeekableByteChannel]]s or [[Seekable]]s, exposes [[SeekableByteChannel.seek]] as well.
 */
trait ByteChannel
  extends ReadableByteChannel {
  protected var _position = 0L

  final def read(dst: ByteBuffer): Int = {
    val n = _read(dst)
    _position += n
    n
  }

  def readString(length: Int, includesNull: Boolean = true): String = {
    val buffer = Buffer(length)
    read(buffer)
    buffer
      .array()
      .slice(
        0,
        if (includesNull)
          length - 1
        else
          length
      )
      .map(_.toChar)
      .mkString("")
  }

  lazy val b4 = Buffer(4)

  def order(order: ByteOrder): Unit =
    b4.order(order)

  def getInt: Int = {
    b4.position(0)
    read(b4)
    b4.getInt(0)
  }

  protected def _read(dst: ByteBuffer): Int

  def read(dst: ByteBuffer, offset: Int, length: Int): Int = {
    dst.position(offset)
    val prevLimit = dst.limit()
    dst.limit(offset + length)
    val n = read(dst)
    dst.limit(prevLimit)
    n
  }

  final def skip(n: Int): Unit = {
    _skip(n)
    _position += n
  }

  protected def _skip(n: Int): Unit

  def position(): Long = _position
}

trait SeekableByteChannel
  extends ByteChannel {
  def seek(newPos: Long): Unit = {
    _position = newPos
    _seek(newPos)
  }

  protected def _seek(newPos: Long): Unit
}

object ByteChannel {

  implicit class ChannelByteChannel(ch: channels.SeekableByteChannel)
    extends SeekableByteChannel {
    override def _read(dst: ByteBuffer): Int = ch.read(dst)
    override def _skip(n: Int): Unit = ch.position(ch.position() + n)
    override def isOpen = ch.isOpen
    override def close(): Unit = { ch.close() }
    override def position(): Long = ch.position()
    override protected def _seek(newPos: Long): Unit = ch.position(newPos)
  }

  implicit class SeekableHadoopByteChannel(is: InputStream with Seekable)
    extends InputStreamByteChannel(is)
      with SeekableByteChannel {
    override protected def _seek(newPos: Long): Unit = is.seek(newPos)
  }

  implicit class IteratorByteChannel(it: Iterator[Byte])
    extends ByteChannel {
    override def _read(dst: ByteBuffer): Int = {
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

    override def _skip(n: Int): Unit = {
      it.drop(n)
    }

    override def isOpen: Boolean = true
    override def close(): Unit = {}
  }

  implicit class InputStreamByteChannel(is: InputStream)
    extends ByteChannel {
    override def _read(dst: ByteBuffer): Int =
      is.read(
        dst.array(),
        dst.position(),
        dst.remaining()
      )

    override def _skip(n: Int): Unit = {
      var remaining = n.toLong
      while (remaining > 0) {
        val skipped = is.skip(remaining)
        if (skipped <= 0)
          throw new IOException(
            s"Only skipped $skipped of $remaining, total $n (${is.available()})"
          )
        remaining -= skipped
      }
    }

    override def isOpen: Boolean = is.isOpen
    override def close(): Unit = is.close()
  }
}
