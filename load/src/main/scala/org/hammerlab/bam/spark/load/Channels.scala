package org.hammerlab.bam.spark.load

import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel.ChannelByteChannel
import org.hammerlab.channel.{ CachingChannel, SeekableByteChannel }
import org.hammerlab.paths.Path

case class Channels(path: Path,
                    compressedChannel: CachingChannel[ChannelByteChannel],
                    uncompressedBytes: SeekableUncompressedBytes) {
  def close(): Unit = uncompressedBytes.close()
}

object Channels {
  def apply(path: Path): Channels = {
    val compressedChannel =
      SeekableByteChannel(path).cache

    val uncompressedBytes =
      SeekableUncompressedBytes(compressedChannel)

    Channels(
      path,
      compressedChannel,
      uncompressedBytes
    )
  }
}
