package org.hammerlab.bam.check.full

import java.io.IOException

import org.apache.spark.broadcast.Broadcast
import org.hammerlab.bam.check
import org.hammerlab.bam.check.Checker.{ MAX_CIGAR_OP, MakeChecker, ReadsToCheck, SuccessfulReads, allowedReadNameChars }
import org.hammerlab.bam.check.CheckerBase
import org.hammerlab.bam.check.full.error._
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.channel.{ CachingChannel, SeekableByteChannel }

/**
 * [[check.Checker]] that builds [[Flags]] of all failing checks at each [[org.hammerlab.bgzf.Pos]].
 */
case class Checker(uncompressedStream: SeekableUncompressedBytes,
                   contigLengths: ContigLengths,
                   readsToCheck: ReadsToCheck)
  extends CheckerBase[Result] {

  override def apply(implicit
                     successfulReads: SuccessfulReads
  ): Result = {

    if (successfulReads.n == readsToCheck.n)
      return Success(readsToCheck.n)

    buf.position(0)
    try {
      uncompressedBytes.readFully(buf)
    } catch {
      case _: IOException ⇒
        return Flags(
          tooFewFixedBlockBytes = true,
          readPosError = None,
          nextReadPosError = None,
          readNameError = None,
          cigarOpsError = None,
          tooFewRemainingBytesImplied = false,
          readsBeforeError = successfulReads.n
        )
    }

    buf.position(0)
    val remainingBytes = buf.getInt

    val readPosError = getRefPosError()

    val readNameLength = buf.getInt & 0xff

    val numCigarOps = buf.getInt & 0xffff
    val numCigarBytes = 4 * numCigarOps

    val seqLen = buf.getInt

    val numSeqAndQualBytes = (seqLen + 1) / 2 + seqLen

    implicit val tooFewRemainingBytesImplied =
      remainingBytes < 32 + readNameLength + numCigarBytes + numSeqAndQualBytes

    val nextReadPosError = getRefPosError()

    implicit val posErrors = (readPosError, nextReadPosError)

    buf.getInt  // unused: template length

    try {
      implicit val readNameError: Option[ReadNameError] =
        readNameLength match {
          case 0 ⇒
            Some(NoReadName)
          case 1 ⇒
            Some(EmptyReadName)
          case _ ⇒
            readNameBuffer.position(0)
            readNameBuffer.limit(readNameLength)
            uncompressedBytes.readFully(readNameBuffer)

            // Drop trailing '\0'
            val readNameBytes =
              readNameBuffer
                .array()
                .view
                .slice(0, readNameLength)

            if (readNameBytes.last != 0)
              Some(NonNullTerminatedReadName)
            else if (
              readNameBytes
                .view
                .slice(0, readNameLength - 1)
                .exists(byte ⇒ !allowedReadNameChars(byte.toChar))
            )
              Some(NonASCIIReadName)
            else
              None
        }

      implicit val cigarOpsError: Option[CigarOpsError] =
        try {
          if (
            (0 until numCigarOps)
              .exists {
                _ ⇒
                  (uncompressedBytes.getInt & 0xf) > MAX_CIGAR_OP
              }
          )
            Some(InvalidCigarOp)
          else
            None
        } catch {
          case _: IOException ⇒
            Some(TooFewBytesForCigarOps)
        }

      return build

    } catch {
      case _: IOException ⇒
        implicit val readNameError = Some(TooFewBytesForReadName)
        return build
    }

    build match {
      case Success(_) ⇒
        apply(
          successfulReads.n + 1
        )
      case flags ⇒ flags
    }
  }

  /**
   * Construct an [[Flags]] from some convenient, implicit wrappers around subsets of the possible flags
   */
  def build(implicit
            posErrors: (Option[RefPosError], Option[RefPosError]),
            readNameError: Option[ReadNameError] = None,
            cigarOpsError: Option[CigarOpsError] = None,
            tooFewRemainingBytesImplied: Boolean = false,
            successfulReads: SuccessfulReads): Result =
    (posErrors, readNameError, cigarOpsError, tooFewRemainingBytesImplied) match {
      case ((None, None), None, None, false) ⇒
        Success(successfulReads.n)
      case _ ⇒
        Flags(
          tooFewFixedBlockBytes = false,
          readPosError = posErrors._1,
          nextReadPosError = posErrors._2,
          readNameError = readNameError,
          cigarOpsError = cigarOpsError,
          tooFewRemainingBytesImplied = tooFewRemainingBytesImplied,
          readsBeforeError = successfulReads.n
        )
    }
}

object Checker {
  implicit def makeChecker(implicit
                           contigLengths: Broadcast[ContigLengths],
                           readsToCheck: ReadsToCheck): MakeChecker[Result, Checker] =
    new MakeChecker[Result, Checker] {
      override def apply(ch: CachingChannel[SeekableByteChannel]): Checker =
        Checker(
          SeekableUncompressedBytes(ch),
          contigLengths.value,
          readsToCheck
        )
    }
}
