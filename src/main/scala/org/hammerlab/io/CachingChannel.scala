package org.hammerlab.io

import java.io.{ EOFException, IOException }
import java.nio.ByteBuffer

import scala.collection.concurrent
import scala.math.{ max, min }

/**
 * [[SeekableByteChannel]] that wraps another [[SeekableByteChannel]] and caches data read from the latter in chunks of
 * size [[blockSize]] (quantized to boundaries at whole multiples of [[blockSize]])
 *
 * @param channel underlying channel to provide a caching layer over
 * @param blockSize size of blocks to cache
 * @param maxReadAttempts all read/skip operations require the full requested number of bytes to be returned in at most
 *                        this many attempts, or they will throw an [[IOException]].
 */
case class CachingChannel[Channel <: SeekableByteChannel](channel: Channel,
                                                          blockSize: Int,
                                                          maxReadAttempts: Int = 2)
  extends SeekableByteChannel {

  private val _buffer = ByteBuffer.allocate(blockSize)

  val blocks = concurrent.TrieMap[Long, ByteBuffer]()

  def getBlock(idx: Long): ByteBuffer =
    blocks.getOrElseUpdate(
      idx,
      {
        _buffer.clear()
        val start = idx * blockSize
        channel.seek(start)
        if (channel.size - start < _buffer.limit) {
          _buffer.limit((channel.size - start).toInt)
        }
        val bytesToRead = _buffer.remaining()
        var attempts = 0
        val end = start + bytesToRead
        while (channel.position() < end && attempts < maxReadAttempts) {
          channel.read(_buffer)
          attempts += 1
        }

        if (channel.position() < end) {
          throw new IOException(
            s"Read ${channel.position() - start} of $bytesToRead bytes from $start in $attempts attempts"
          )
        }

        val dupe = ByteBuffer.allocate(bytesToRead)
        _buffer.position(0)
        dupe.put(_buffer)
      }
    )

  def ensureBlocks(from: Long, to: Long): Unit =
    for {
      idx ← from to to
    } {
      getBlock(idx)
    }


  override def size: Long = channel.size

  override def _seek(newPos: Long): Unit = channel._seek(newPos)

  override protected def _read(dst: ByteBuffer): Unit = {
    val start = position()
    val end = start + dst.remaining()
    if (end > channel.size)
      throw new EOFException

    val startBlock = start / blockSize
    val endBlock = end / blockSize

    ensureBlocks(startBlock, endBlock)

    for {
      idx ← startBlock to endBlock
      blockStart = idx * blockSize
      blockEnd = (idx + 1) * blockSize
      from = max((start - blockStart).toInt, 0)
      to = (min(end, blockEnd) - blockStart).toInt
      blockBuffer = blocks(idx)
    } {
      blockBuffer.position(from)
      blockBuffer.limit(to)
      dst.put(blockBuffer)
      blockBuffer.clear()
    }
  }

  override def close(): Unit = {
    super.close()
    channel.close()
  }
}

object CachingChannel {

  case class Config(blockSize: Int = 64 * 1024,
                    maxReadAttempts: Int = 2)

  object Config {
    implicit val default = Config()
  }

  implicit def makeCachingChannel[Channel <: SeekableByteChannel](channel: Channel)(
      implicit config: Config
  ): CachingChannel[Channel] =
    CachingChannel(
      channel,
      config.blockSize,
      config.maxReadAttempts
    )

  implicit class AddCaching[Channel <: SeekableByteChannel](channel: Channel) {
    def cache(implicit config: Config): CachingChannel[Channel] =
      makeCachingChannel(channel)
  }
}
