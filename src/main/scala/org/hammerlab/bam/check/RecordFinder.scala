package org.hammerlab.bam.check

import java.io.IOException

import org.hammerlab.bam.check.Error.Flags
import org.hammerlab.bam.check.RecordFinder.{ FIXED_FIELDS_SIZE, allowedReadNameChars }
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.io.{ Buffer, ByteChannel }

class RecordFinder {
  val buf = Buffer(FIXED_FIELDS_SIZE)
  val readNameBuffer = Buffer(255)

  def apply(ch: ByteChannel,
            contigLengths: Map[Int, NumLoci]): Option[Flags] = {

    buf.position(0)
    try {
      ch.read(buf)
    } catch {
      case _: IOException ⇒
        return Some(
          Error(
            tooFewFixedBlockBytes = true,
            None, None, None, None, false
          )
        )
    }

    buf.position(0)
    val remainingBytes = buf.getInt

    def getRefPosError: Option[RefPosError] = {
      val refIdx = buf.getInt
      val refPos = buf.getInt

      if (refIdx < -1)
        if (refPos < -1)
          Some(NegativeRefIdxAndPos)
        else
          Some(NegativeRefIdx)
      else if (refIdx >= contigLengths.size)
        if (refPos < -1)
          Some(TooLargeRefIdxNegativePos)
        else
          Some(TooLargeRefIdx)
      else if (refPos < -1)
        Some(NegativeRefPos)
      else if (refIdx >= 0 && refPos > contigLengths(refIdx))
        Some(TooLargeRefPos)
      else
        None
    }

    val readPosError = getRefPosError

    val readNameLength = buf.getInt & 0xff

    val numCigarOps = buf.getInt & 0xffff
    val numCigarBytes = 4 * numCigarOps

    val seqLen = buf.getInt

    val numSeqAndQualBytes = (seqLen + 1) / 2 + seqLen

    implicit val tooFewRemainingBytesImplied =
      remainingBytes < 32 + readNameLength + numCigarBytes + numSeqAndQualBytes

    val nextReadPosError = getRefPosError

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
            if (readNameLength < 0 || readNameLength > 255) {
              println(s"uh oh: $readNameLength")
            }
            readNameBuffer.limit(readNameLength)
            ch.read(readNameBuffer)
            val readNameBytes = readNameBuffer.array().view.slice(0, readNameLength)

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

      return Error.build

    } catch {
      case _: IOException ⇒
        implicit val readNameError = Some(TooFewBytesForReadName)
        return Error.build
    }

    Error.build
  }
}

object RecordFinder {

  val allowedReadNameChars =
    (
      ('!' to '?') ++
      ('A' to '~')
    )
    .toSet

  val FIXED_FIELDS_SIZE = 9 * 4  // 9 4-byte ints at the start of every BAM record
}
