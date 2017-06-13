package org.hammerlab.bam.check.full

import java.io.IOException

import org.hammerlab.bam.check.Checker.allowedReadNameChars
import org.hammerlab.bam.check.full.error.{ CigarOpsError, EmptyReadName, Flags, InvalidCigarOp, NoReadName, NonASCIIReadName, NonNullTerminatedReadName, ReadNameError, RefPosError, TooFewBytesForCigarOps, TooFewBytesForReadName }
import org.hammerlab.bam.check
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.block.SeekableByteStream

/**
 * [[check.Checker]] that builds [[Flags]] of all failing checks at each [[org.hammerlab.bgzf.Pos]].
 */
case class Checker(uncompressedStream: SeekableByteStream,
                   contigLengths: ContigLengths)
  extends check.Checker[Option[Flags]] {

  override def apply(remainingBytes: Int): Option[Flags] = {

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
            ch.read(readNameBuffer)

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
                (ch.getInt & 0xf) > 8
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

    build
  }

  override def tooFewFixedBlockBytes: Option[Flags] =
    Some(
      Flags(
        tooFewFixedBlockBytes = true,
        None, None, None, None, false
      )
    )

  /**
   * Construct an [[Flags]] from some convenient, implicit wrappers around subsets of the possible flags
   */
  def build(implicit
            posErrors: (Option[RefPosError], Option[RefPosError]),
            readNameError: Option[ReadNameError] = None,
            cigarOpsError: Option[CigarOpsError] = None,
            tooFewRemainingBytesImplied: Boolean = false): Option[Flags] =
    (posErrors, readNameError, cigarOpsError, tooFewRemainingBytesImplied) match {
      case ((None, None), None, None, false) ⇒
        None
      case _ ⇒
        Some(
          Flags(
            tooFewFixedBlockBytes = false,
            readPosError = posErrors._1,
            nextReadPosError = posErrors._2,
            readNameError = readNameError,
            cigarOpsError = cigarOpsError,
            tooFewRemainingBytesImplied = tooFewRemainingBytesImplied
          )
        )
    }
}
