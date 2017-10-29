package org.hammerlab.bam.check

import caseapp.core.Default
import org.hammerlab.bgzf.Pos
import org.hammerlab.channel.{ CachingChannel, SeekableByteChannel }

trait Checker[+Call] {
  def apply(pos: Pos): Call
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

  def default[T]()(implicit d: Default[T]): T = d.apply()
}
