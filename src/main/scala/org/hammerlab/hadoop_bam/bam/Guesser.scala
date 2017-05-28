package org.hammerlab.hadoop_bam.bam

import java.nio.ByteBuffer

import org.hammerlab.genomics.reference.NumLoci

import scala.math.max

case class Error(tooFewFixedBlockBytes: Boolean,
                 readPosError: Option[RefPosError],
                 nextReadPosError: Option[RefPosError],
                 readNameError: Option[ReadNameError],
                 cigarOpsError: Option[CigarOpsError],
                 tooFewBytesForSeqAndQuals: Boolean) {
  def negativeReadIdx = readPosError.exists(_.negativeRefIdx)
  def tooLargeReadIdx = readPosError.exists(_.tooLargeRefIdx)
  def negativeReadPos = readPosError.exists(_.negativeRefPos)
  def tooLargeReadPos = readPosError.exists(_.tooLargeRefPos)

  def negativeNextReadIdx = nextReadPosError.exists(_.negativeRefIdx)
  def tooLargeNextReadIdx = nextReadPosError.exists(_.tooLargeRefIdx)
  def negativeNextReadPos = nextReadPosError.exists(_.negativeRefPos)
  def tooLargeNextReadPos = nextReadPosError.exists(_.tooLargeRefPos)

  def nonNullTerminatedReadName = readNameError.exists(_.nonNullTerminatedReadName)
  def nonASCIIReadName = readNameError.exists(_.nonASCIIReadName)
  def noReadName = readNameError.exists(_.noReadName)
  def emptyReadName = readNameError.exists(_.emptyReadName)

  def tooFewBytesForCigarOps = cigarOpsError.exists(_.tooFewBytesForCigarOps)
  def invalidCigarOp = cigarOpsError.exists(_.invalidCigarOp)
}

object Error {
  def apply(implicit
            posErrors: (Option[RefPosError], Option[RefPosError]),
            readNameError: Option[ReadNameError] = None,
            cigarOpsError: Option[CigarOpsError] = None,
            tooFewBytesForSeqAndQuals: Boolean = false): Option[Error] =
    (posErrors, readNameError, cigarOpsError, tooFewBytesForSeqAndQuals) match {
      case ((None, None), None, None, false) ⇒ None
      case _ ⇒
        Some(
          Error(
            tooFewFixedBlockBytes = false,
            readPosError = posErrors._1,
            nextReadPosError = posErrors._2,
            readNameError = readNameError,
            cigarOpsError = cigarOpsError,
            tooFewBytesForSeqAndQuals = tooFewBytesForSeqAndQuals
          )
        )
    }
}

sealed trait RefPosError {
  def negativeRefIdx: Boolean = false
  def tooLargeRefIdx: Boolean = false
  def negativeRefPos: Boolean = false
  def tooLargeRefPos: Boolean = false
}

  sealed trait NegativeRefIdx extends RefPosError {
    override def negativeRefIdx = true
  }
    case object NegativeRefIdx extends NegativeRefIdx
    case object NegativeRefIdxAndPos extends NegativeRefIdx with NegativeRefPos

  sealed trait TooLargeRefIdx extends RefPosError {
    override def tooLargeRefIdx = true
  }
    case object TooLargeRefIdx extends TooLargeRefIdx
    case object TooLargeRefIdxNegativePos extends TooLargeRefIdx with NegativeRefPos

  sealed trait NegativeRefPos extends RefPosError {
    override def negativeRefPos = true
  }
    case object NegativeRefPos extends NegativeRefPos
  case object TooLargeRefPos extends RefPosError {
    override def tooLargeRefPos = true
  }

  sealed trait ReadNameError {
    def tooFewBytesForReadName = false
    def nonNullTerminatedReadName = false
    def nonASCIIReadName = false
    def noReadName = false
    def emptyReadName = false
  }
    case object TooFewBytesForReadName
      extends ReadNameError {
      override def tooFewBytesForReadName = true
    }
    case object NonNullTerminatedReadName extends ReadNameError {
      override def nonNullTerminatedReadName = true
    }
    case object NonASCIIReadName extends ReadNameError {
      override def nonASCIIReadName = true
    }
    sealed trait ReadNameLengthError extends ReadNameError
    case object NoReadName extends ReadNameLengthError {
      override def noReadName = true
    }
    case object EmptyReadName extends ReadNameLengthError {
      override def emptyReadName = true
    }

    sealed trait CigarOpsError {
      def invalidCigarOp = false
      def tooFewBytesForCigarOps = false
    }
      case object InvalidCigarOp extends CigarOpsError {
        override def invalidCigarOp = true
      }
      case object TooFewBytesForCigarOps
        extends CigarOpsError {
        override def tooFewBytesForCigarOps = true
      }

object Guesser {

  val allowedReadNameChars =
    (
      ('a' to 'z') ++
        ('A' to 'Z') ++
        ('0' to '9') ++
        """ -.,\/|_=+!@#$%^&*(){}[]<>?:;"'"""
    )
    .toSet

  def guess(buf: ByteBuffer,
            contigLengths: Map[Int, NumLoci]): Option[Error] = {
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

    val impliedRemainingBytes =
      32 +
        max(0, readNameLength) +
        numCigarBytes +
        (seqLen + 1) / 2 +  // bases
        seqLen              // phred scores

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
