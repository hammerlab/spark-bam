package org.hammerlab.io

import java.io.{ IOException, InputStream }
import java.nio.ByteBuffer
import java.nio.channels.{ ReadableByteChannel, SeekableByteChannel }

/**
 * Readable, seekable interface over [[InputStream]]s, [[Iterator[Byte]]]s, and [[SeekableByteChannel]]s.
 */
trait ByteChannel
  extends ReadableByteChannel {
  private var _position = 0L

  final def read(dst: ByteBuffer): Int = {
    val n = _read(dst)
    _position += n
    n
  }
  protected def _read(dst: ByteBuffer): Int

  final def skip(n: Int): Unit = {
    _skip(n)
    _position += n
  }
  protected def _skip(n: Int): Unit

  def position(): Long = {
    _position
  }
}

object ByteChannel {

  implicit class ChannelByteChannel(ch: SeekableByteChannel)
    extends ByteChannel {
    override def _read(dst: ByteBuffer): Int = ch.read(dst)
    override def _skip(n: Int): Unit = ch.position(ch.position() + n)
    override def isOpen = ch.isOpen
    override def close(): Unit = { ch.close() }
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
        dst.limit()
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
