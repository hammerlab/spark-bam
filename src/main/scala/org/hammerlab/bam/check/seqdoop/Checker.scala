package org.hammerlab.bam.check.seqdoop

import java.io.Closeable

import htsjdk.samtools.SAMFormatException
import htsjdk.samtools.seekablestream.SeekableStream
import htsjdk.samtools.util.RuntimeIOException
import org.hammerlab.bam.check
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.hadoop.Configuration
import org.hammerlab.io.CachingChannel._
import org.hammerlab.io.SeekableByteChannel
import org.hammerlab.paths.Path
import org.seqdoop.hadoop_bam.BAMPosGuesser
import org.seqdoop.hadoop_bam.BAMSplitGuesser.MAX_BYTES_READ

import scala.math.min

case class Checker(path: Path,
                   contigLengths: ContigLengths)(
    implicit conf: Configuration
)
  extends check.Checker[Boolean]
    with Closeable {

  val cachingChannel = SeekableByteChannel(path).cache

  /** Wrap block-caching input stream in an HTSJDK [[SeekableStream]] for consumption by [[BAMPosGuesser]] */
  val ss = TruncatableSeekableStream(cachingChannel, path)

  val guesser =
    new BAMPosGuesser(
      ss,
      contigLengths.size
    )

  override def apply(pos: Pos): Boolean =
    try {
      ss.limit =
        min(
          cachingChannel.size,
          pos.blockPos + MAX_BYTES_READ
        )

      guesser.checkRecordStart(pos.toHTSJDK) &&
        guesser.checkSucceedingRecords(pos.toHTSJDK)
    } catch {
      case e: SAMFormatException ⇒
        throw BadBlockPos(pos, e)
      case e: RuntimeIOException ⇒
        throw BadBlockPos(pos, e)
    }

  override def close(): Unit =
    ss.close()
}

case class BadBlockPos(pos: Pos, e: RuntimeException)
  extends Exception(
    s"Failed to parse block at $pos",
    e
  )
