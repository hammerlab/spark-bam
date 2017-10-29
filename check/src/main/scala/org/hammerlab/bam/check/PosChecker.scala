package org.hammerlab.bam.check

import org.hammerlab.bam.check.Checker.FIXED_FIELDS_SIZE
import org.hammerlab.bam.check.full.error.{ NegativeRefIdx, NegativeRefIdxAndPos, NegativeRefPos, RefPosError, TooLargeRefIdx, TooLargeRefIdxNegativePos, TooLargeRefPos }
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.channel.ByteChannel
import org.hammerlab.io.Buffer

/**
 * Functionality shared by [[eager]] and [[full]] [[Checker]]s, which do the same checks but differ in what information
 * they hold on to and return.
 */
trait PosChecker[Call]
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
    uncompressedStream.seek(pos)
    apply(uncompressedBytes.position())(0)
  }

  protected def apply(startPos: Long)(
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
