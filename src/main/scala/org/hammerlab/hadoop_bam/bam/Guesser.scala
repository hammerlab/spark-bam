package org.hammerlab.hadoop_bam.bam

import java.nio.ByteBuffer

import org.hammerlab.genomics.reference.NumLoci

import scala.math.max

sealed trait Error
  case object TooFewFixedBlockBytes extends Error
  sealed trait PosErrors extends Error
    case class ReadPosError(refPosError: RefPosError) extends PosErrors
    case class NextReadPosError(refPosError: RefPosError) extends PosErrors
    case class RefPosErrors(refPosError: RefPosError,
                            nextRefPosError: RefPosError) extends PosErrors

      sealed trait RefPosError
        sealed trait RefIdxError extends RefPosError

          sealed trait NegativeRefIdx extends RefIdxError
            case object NegativeRefIdx extends NegativeRefIdx
            case object NegativeRefIdxAndPos extends NegativeRefIdx with NegativeRefPos

          sealed trait TooLargeRefIdx extends RefIdxError
            case object TooLargeRefIdx extends TooLargeRefIdx
            case object TooLargeRefIdxNegativePos extends TooLargeRefIdx with NegativeRefPos

        sealed trait RefLocusError extends RefPosError
          sealed trait NegativeRefPos extends RefLocusError
            case object NegativeRefPos extends NegativeRefPos
          case object TooLargeRefPos extends RefLocusError

  sealed trait VariableFieldErrors extends Error
    case object TooFewBytesForReadName extends VariableFieldErrors
    case class TooFewBytesForCigarOps(readNameError: Option[ReadNameError]) extends VariableFieldErrors
    case class TooFewBytesForSeqAndQuals(readNameError: Option[ReadNameError],
                                         cigarOpsError: Option[CigarOpsError]) extends VariableFieldErrors
    case class ReadNameAndCigarOpsErrors(readNameError: ReadNameError,
                                         cigarOpsError: CigarOpsError) extends VariableFieldErrors

    sealed trait ReadNameError extends VariableFieldErrors
      case object NonNullTerminatedReadName extends ReadNameError
      case object NonASCIIReadName extends ReadNameError
      sealed trait ReadNameLengthError extends ReadNameError
        case object NoReadName extends ReadNameLengthError
        case object EmptyReadName extends ReadNameLengthError

    sealed trait CigarOpsError extends VariableFieldErrors
      case object InvalidCigarOp extends CigarOpsError

  case class PosAndVariableErrors(posErrors: PosErrors,
                                  variableErrors: VariableFieldErrors) extends Error

object Guesser {
//  def guess(bytes: Array[Byte],
//            contigLengths: Map[Int, NumLoci],
//            numTries: Int = 64 * 1024): Option[Guess] = {
//    var i = 0
//    val buf = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN)
//    while (i < numTries) {
//      buf.position(i)
//      guess(buf, contigLengths) match {
//        case Some(g) ⇒
//          return Some(g)
//        case _ ⇒
//      }
//      i += 1
//    }
//    None
//  }

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
      return Some(TooFewFixedBlockBytes)

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

    val refPosError = getRefPosError

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

    val nextRefPosError = getRefPosError

    val posErrors: Option[PosErrors] =
      (refPosError, nextRefPosError) match {
        case (Some(refPosError), Some(nextRefPosError)) ⇒
          Some(RefPosErrors(refPosError, nextRefPosError))
        case (Some(refPosError), _) ⇒
          Some(
            ReadPosError(
              refPosError
            )
          )
        case (_, Some(nextRefPosError)) ⇒
          Some(
            NextReadPosError(
              nextRefPosError
            )
          )
        case _ ⇒
          None
      }

    buf.getInt  // unused: template length

    val variableFieldsError: Option[VariableFieldErrors] =
      if (buf.remaining() < readNameLength)
        Some(TooFewBytesForReadName)
      else {
        val readNameBytes = Array.fill[Byte](readNameLength)(0)
        buf.get(readNameBytes)

        val readNameError: Option[ReadNameError] =
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

        if (buf.remaining() < numCigarBytes)
          Some(
            TooFewBytesForCigarOps(
              readNameError
            )
          )
        else {

          val cigarOpsError: Option[CigarOpsError] =
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

          if (buf.remaining() < (seqLen + 1) / 2 + seqLen)
            Some(
              TooFewBytesForSeqAndQuals(
                readNameError,
                cigarOpsError
              )
            )
          else
            (readNameError, cigarOpsError) match {
              case (Some(readNameError), Some(cigarOpsError)) ⇒
                Some(
                  ReadNameAndCigarOpsErrors(
                    readNameError,
                    cigarOpsError
                  )
                )
              case (Some(readNameError), _) ⇒
                Some(readNameError)
              case (_, Some(cigarOpsError)) ⇒
                Some(cigarOpsError)
              case _ ⇒
                None
            }
        }
      }

    (posErrors, variableFieldsError) match {
      case (Some(posErrors), Some(variableFieldsError)) ⇒
        Some(
          PosAndVariableErrors(
            posErrors,
            variableFieldsError
          )
        )
      case (Some(posErrors), _) ⇒
        Some(posErrors)
      case (_, Some(variableFieldsError)) ⇒
        Some(variableFieldsError)
      case _ ⇒
        None
    }
  }
}
