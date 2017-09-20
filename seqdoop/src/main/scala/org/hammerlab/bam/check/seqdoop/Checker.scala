package org.hammerlab.bam.check.seqdoop

import java.io.Closeable

import htsjdk.samtools.SAMFormatException
import htsjdk.samtools.seekablestream.SeekableStream
import htsjdk.samtools.util.RuntimeIOException
import org.apache.spark.broadcast.Broadcast
import org.hammerlab.bam.check
import org.hammerlab.bam.check.Checker.MakeChecker
import org.hammerlab.bam.check.ReadStartFinder
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.channel.{ CachingChannel, SeekableByteChannel }
import org.hammerlab.paths.Path
import org.seqdoop.hadoop_bam.BAMSplitGuesser.MAX_BYTES_READ
import org.seqdoop.hadoop_bam.{ BAMPosGuesser, BAMSplitGuesser }

import scala.math.min

case class Checker(path: Path,
                   cachingChannel: CachingChannel[SeekableByteChannel],
                   contigLengths: ContigLengths)
  extends ReadStartFinder
    with Closeable {

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

  @transient lazy val pathSize = path.size

  @transient lazy val splitGuesser = new BAMSplitGuesser(ss, contigLengths.size)

  override def nextReadStart(start: Pos)(implicit maxReadSize: check.MaxReadSize): Option[Pos] = {
    val end = min(start.blockPos + MAX_BYTES_READ, pathSize)
    splitGuesser.guessNextBAMRecordStart(start.blockPos, end) match {
      case guess if guess == end ⇒ None
      case guess ⇒ Some(Pos(guess))
    }
  }
}

case class BadBlockPos(pos: Pos, e: RuntimeException)
  extends Exception(
    s"Failed to parse block at $pos",
    e
  )

object Checker {
  implicit def makeChecker(implicit
                           path: Path,
                           contigLengths: Broadcast[ContigLengths]): MakeChecker[Boolean, Checker] =
    new MakeChecker[Boolean, Checker] {
      override def apply(ch: CachingChannel[SeekableByteChannel]): Checker =
        Checker(
          path,
          ch,
          contigLengths.value
        )
    }
}
