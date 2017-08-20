package org.hammerlab.bam.check

import caseapp.core.{ ArgParser, Default }
import caseapp.core.ArgParser.instance
import org.hammerlab.bam.check.Checker.{ FIXED_FIELDS_SIZE, ReadsToCheck, SuccessfulReads }
import org.hammerlab.bam.check.full.error.{ NegativeRefIdx, NegativeRefIdxAndPos, NegativeRefPos, RefPosError, TooLargeRefIdx, TooLargeRefIdxNegativePos, TooLargeRefPos }
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.channel.{ ByteChannel, CachingChannel, SeekableByteChannel }
import org.hammerlab.io.Buffer

trait Checker[+Call] {
  def apply(pos: Pos): Call
}

trait CheckerBase[Call]
  extends Checker[Call] {

  // Buffers (re-)used for reading presumptive BAM records
  val buf = Buffer(FIXED_FIELDS_SIZE)
  val readNameBuffer = Buffer(255)

  def uncompressedStream: SeekableUncompressedBytes
  def contigLengths: ContigLengths

  lazy val uncompressedBytes: ByteChannel = uncompressedStream

  def seek(pos: Pos): Unit = uncompressedStream.seek(pos)

  def readsToCheck: ReadsToCheck

  /** Main record-checking entry-point */
  override def apply(pos: Pos): Call = {
    seek(pos)
    apply(0)
  }

  def apply(): Call = apply(0)

  def apply(
      implicit
      successfulReads: SuccessfulReads
  ): Call

  /** Reusable logic for fetching a reference-sequence index and reference-position */
  def getRefPosError(): Option[RefPosError] = {
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
    else if (refIdx >= 0 && refPos.toLong > contigLengths(refIdx)._2)
      Some(TooLargeRefPos)
    else
      None
  }
}

object Checker {
  val allowedReadNameChars =
    (
      ('!' to '?') ++
      ('A' to '~')
    )
    .toSet

  val FIXED_FIELDS_SIZE = 9 * 4  // 9 4-byte ints at the start of every BAM record

  val MAX_CIGAR_OP = 8

  trait MakeChecker[Call, C <: Checker[Call]]
    extends ((CachingChannel[SeekableByteChannel]) ⇒ C)
      with Serializable

  implicit class SuccessfulReads(val n: Int) extends AnyVal

  implicit class BGZFBlocksToCheck(val n: Int) extends AnyVal
  object BGZFBlocksToCheck {
    implicit val parser: ArgParser[BGZFBlocksToCheck] =
      instance("bgzf-blocks-to-check") {
        str ⇒ Right(str.toInt)
      }

    implicit val default: Default[BGZFBlocksToCheck] =
      Default.instance(5)
  }

  implicit class ReadsToCheck(val n: Int) extends AnyVal
  object ReadsToCheck {
    implicit val parser: ArgParser[ReadsToCheck] =
      instance("reads-to-check") {
        str ⇒ Right(str.toInt)
      }

    implicit val default: Default[ReadsToCheck] =
      Default.instance(10)
  }

  implicit class MaxReadSize(val n: Int) extends AnyVal
  object MaxReadSize {
    implicit val parser: ArgParser[MaxReadSize] =
      instance[MaxReadSize]("max-read-size") {
        str ⇒ Right(str.toInt)
      }

    implicit val default: Default[MaxReadSize] =
      Default.instance(10000000)
  }

  def default[T]()(implicit d: Default[T]): T = d.apply()

}
