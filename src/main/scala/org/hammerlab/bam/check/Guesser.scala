package org.hammerlab.bam.check

import java.nio.ByteBuffer

import org.hammerlab.genomics.reference.NumLoci

object Guesser {

  import Error.Flags

  val allowedReadNameChars =
    (
      ('a' to 'z') ++
        ('A' to 'Z') ++
        ('0' to '9') ++
        """ -.,\/|_=+!@#$%^&*(){}[]<>?:;"'"""
    )
    .toSet

  def guess(buf: ByteBuffer,
            contigLengths: Map[Int, NumLoci]): Option[Flags] = {
    if (buf.remaining() < 36)
      return Some(
        Error(
          tooFewFixedBlockBytes = true,
          None, None, None, None, false
        )
      )

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

    val nextReadPosError = getRefPosError

    implicit val posErrors = (readPosError, nextReadPosError)

    buf.getInt  // unused: template length

    if (buf.remaining() < readNameLength) {
      implicit val readNameError = Some(TooFewBytesForReadName)
      Error.apply
    } else {
      val readNameBytes = Array.fill[Byte](readNameLength)(0)
      buf.get(readNameBytes)

      implicit val readNameError: Option[ReadNameError] =
        readNameLength match {
          case 0 ⇒
            Some(NoReadName)
          case 1 ⇒
            Some(EmptyReadName)
          case _ ⇒
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

      if (buf.remaining() < numCigarBytes) {
        implicit val cigarOpsError = Some(TooFewBytesForCigarOps)
        Error.apply
      } else {

        implicit val cigarOpsError: Option[CigarOpsError] =
          if (
            (0 until numCigarOps)
              .exists {
                _ ⇒
                  (buf.getInt & 0xf) > 8
              }
          )
            Some(InvalidCigarOp)
          else
            None

        implicit val tooFewBytesForSeqAndQuals = buf.remaining() < (seqLen + 1) / 2 + seqLen

        Error.apply
      }
    }
  }
}
