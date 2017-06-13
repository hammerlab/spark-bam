package org.hammerlab.bam.check

import java.io.IOException

import org.hammerlab.bam.check.Checker.FIXED_FIELDS_SIZE
import org.hammerlab.bam.check.full.error.{ NegativeRefIdx, NegativeRefIdxAndPos, NegativeRefPos, RefPosError, TooLargeRefIdx, TooLargeRefIdxNegativePos, TooLargeRefPos }
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableByteStream
import org.hammerlab.io.{ Buffer, ByteChannel }

trait Checker[Call] {

  // Buffers (re-)used for reading presumptive BAM records
  val buf = Buffer(FIXED_FIELDS_SIZE)
  val readNameBuffer = Buffer(255)

  def uncompressedStream: SeekableByteStream
  def contigLengths: ContigLengths

  lazy val ch: ByteChannel = uncompressedStream

  def seek(pos: Pos): Unit = uncompressedStream.seek(pos)

  /**
   * Special-cased [[Call]] for when there are fewer than [[org.hammerlab.bam.check.Checker.FIXED_FIELDS_SIZE]] bytes
   * remaining in [[uncompressedStream]], which case is handled here in the superclass.
   */
  def tooFewFixedBlockBytes: Call

  def apply(): Call = {
    buf.position(0)
    try {
      ch.read(buf)
    } catch {
      case _: IOException ⇒
        return tooFewFixedBlockBytes
    }

    buf.position(0)
    val remainingBytes = buf.getInt

    apply(remainingBytes)
  }

  /** Main record-checking entry-point */
  def apply(remainingBytes: Int): Call

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
}
