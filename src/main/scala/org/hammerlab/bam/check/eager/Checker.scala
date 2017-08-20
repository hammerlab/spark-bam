package org.hammerlab.bam.check.eager

import java.io.IOException

import org.apache.spark.broadcast.Broadcast
import org.hammerlab.bam.check
import org.hammerlab.bam.check.Checker.{ MAX_CIGAR_OP, MakeChecker, ReadsToCheck, SuccessfulReads, allowedReadNameChars }
import org.hammerlab.bam.check.CheckerBase
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.channel.{ CachingChannel, SeekableByteChannel }

/**
 * [[check.Checker]] implementation that emits a [[Boolean]] at each [[org.hammerlab.bgzf.Pos]] indicating whether it is
 * a read-record boundary.
 */
case class Checker(uncompressedStream: SeekableUncompressedBytes,
                   contigLengths: ContigLengths,
                   readsToCheck: ReadsToCheck)
  extends CheckerBase[Boolean] {

  override def apply(implicit
                     successfulReads: SuccessfulReads): Boolean = {

    if (successfulReads.n == readsToCheck.n)
      return true

    buf.position(0)
    try {
      uncompressedBytes.readFully(buf)
    } catch {
      case _: IOException ⇒
        return false
    }

    buf.position(0)
    val remainingBytes = buf.getInt

    if (getRefPosError().isDefined)
      return false

    val readNameLength = buf.getInt & 0xff
    readNameLength match {
      case 0 | 1 ⇒
        return false
      case _ ⇒
    }

    val numCigarOps = buf.getInt & 0xffff
    val numCigarBytes = 4 * numCigarOps

    val seqLen = buf.getInt

    val numSeqAndQualBytes = (seqLen + 1) / 2 + seqLen

    if(remainingBytes < 32 + readNameLength + numCigarBytes + numSeqAndQualBytes)
      return false

    if (getRefPosError().isDefined)
      return false

    buf.getInt  // unused: template length

    try {
      readNameBuffer.position(0)
      readNameBuffer.limit(readNameLength)
      uncompressedBytes.readFully(readNameBuffer)
      val readNameBytes = readNameBuffer.array().view.slice(0, readNameLength)

      if (readNameBytes.last != 0)
        return false
      else if (
        readNameBytes
          .view
          .slice(0, readNameLength - 1)
          .exists(byte ⇒ !allowedReadNameChars(byte.toChar))
      )
        return false

      try {
        if (
          (0 until numCigarOps)
            .exists {
              _ ⇒
                (uncompressedBytes.getInt & 0xf) > MAX_CIGAR_OP
            }
        )
          return false
      } catch {
        case _: IOException ⇒
          return false
      }

    } catch {
      case _: IOException ⇒
        return false
    }

    true
  }
}

object Checker {
  implicit def makeChecker(implicit
                           contigLengths: Broadcast[ContigLengths],
                           readsToCheck: ReadsToCheck): MakeChecker[Boolean, Checker] =
    new MakeChecker[Boolean, Checker] {
      override def apply(ch: CachingChannel[SeekableByteChannel]): Checker =
        Checker(
          SeekableUncompressedBytes(ch),
          contigLengths.value,
          readsToCheck
        )
    }
}
