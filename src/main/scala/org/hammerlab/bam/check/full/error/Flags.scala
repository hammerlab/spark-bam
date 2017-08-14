package org.hammerlab.bam.check.full.error

import cats.Show
import cats.Show.show
import shapeless.ops.hlist.Length
import shapeless.{ Generic, LabelledGeneric, Poly1 }

import scala.collection.immutable.BitSet

/**
 * Information about BAM-record checks at a [[org.hammerlab.bgzf.Pos]].
 */
case class Flags(tooFewFixedBlockBytes: Boolean,
                 negativeReadIdx: Boolean,
                 tooLargeReadIdx: Boolean,
                 negativeReadPos: Boolean,
                 tooLargeReadPos: Boolean,
                 negativeNextReadIdx: Boolean,
                 tooLargeNextReadIdx: Boolean,
                 negativeNextReadPos: Boolean,
                 tooLargeNextReadPos: Boolean,
                 tooFewBytesForReadName: Boolean,
                 nonNullTerminatedReadName: Boolean,
                 nonASCIIReadName: Boolean,
                 noReadName: Boolean,
                 emptyReadName: Boolean,
                 tooFewBytesForCigarOps: Boolean,
                 invalidCigarOp: Boolean,
                 tooFewRemainingBytesImplied: Boolean)
  extends Error[Boolean]

object Flags {
  object toLong extends Poly1 {
    implicit val cs: Case.Aux[Boolean, Long] =
      at(
        b ⇒
          if (b)
            1
          else
            0
      )
  }

  val gen = Generic[Flags]

  val size: Int = Length[gen.Repr].apply().toInt

  /**
   * Convert an [[Flags]] to an [[Counts]] by changing true/false to [[1L]]/[[0L]]
   */
  implicit def toCounts(flags: Flags): Counts =
    Generic[Counts]
      .from(
        gen
          .to(flags)
          .map(toLong)
      )

  implicit class FlagsWrapper(val flags: Flags) extends AnyVal {
    /**
     * Count the number of non-zero fields in an [[Counts]]
     */
    def numNonZeroFields: Int =
      gen
        .to(flags)
        .toList[Boolean]
        .count(x ⇒ x)
  }

  private val lg = LabelledGeneric[Flags]

  implicit def makeShow: Show[Flags] =
    show {
      flags ⇒
        import shapeless._
        import ops.record._

        val trueFields: List[String] =
          Fields[lg.Repr]
            .apply(lg.to(flags))
            .toList
            .filter(_._2)
            .map {
              case (k, v) ⇒
                k.name
            }

        trueFields.mkString(",")
    }

  /**
   * Construct an [[Flags]] from some convenient wrappers around subsets of the possible flags
   */
  def apply(tooFewFixedBlockBytes: Boolean,
            readPosError: Option[RefPosError],
            nextReadPosError: Option[RefPosError],
            readNameError: Option[ReadNameError],
            cigarOpsError: Option[CigarOpsError],
            tooFewRemainingBytesImplied: Boolean): Flags =
    Flags(
      tooFewFixedBlockBytes = tooFewFixedBlockBytes,

      negativeReadIdx = readPosError.exists(_.negativeRefIdx),
      tooLargeReadIdx = readPosError.exists(_.tooLargeRefIdx),
      negativeReadPos = readPosError.exists(_.negativeRefPos),
      tooLargeReadPos = readPosError.exists(_.tooLargeRefPos),

      negativeNextReadIdx = nextReadPosError.exists(_.negativeRefIdx),
      tooLargeNextReadIdx = nextReadPosError.exists(_.tooLargeRefIdx),
      negativeNextReadPos = nextReadPosError.exists(_.negativeRefPos),
      tooLargeNextReadPos = nextReadPosError.exists(_.tooLargeRefPos),

      tooFewBytesForReadName = readNameError.exists(_.tooFewBytesForReadName),
      nonNullTerminatedReadName = readNameError.exists(_.nonNullTerminatedReadName),
      nonASCIIReadName = readNameError.exists(_.nonASCIIReadName),
      noReadName = readNameError.exists(_.noReadName),
      emptyReadName = readNameError.exists(_.emptyReadName),

      tooFewBytesForCigarOps = cigarOpsError.exists(_.tooFewBytesForCigarOps),
      invalidCigarOp = cigarOpsError.exists(_.invalidCigarOp),
      tooFewRemainingBytesImplied = tooFewRemainingBytesImplied
    )

  /** Convert to and from a [[BitSet]] during serialization. */
  implicit def toBitSet(flags: Flags): BitSet =
    BitSet(
      Generic[Flags]
        .to(flags)
        .toList[Boolean]
        .zipWithIndex
        .flatMap {
          case (flag, idx) ⇒
            if (flag)
              Some(idx)
            else
              None
        }: _*
    )

  implicit def fromBitSet(flags: BitSet): Flags =
    Flags(
      tooFewFixedBlockBytes       = flags( 0),
      negativeReadIdx             = flags( 1),
      tooLargeReadIdx             = flags( 2),
      negativeReadPos             = flags( 3),
      tooLargeReadPos             = flags( 4),
      negativeNextReadIdx         = flags( 5),
      tooLargeNextReadIdx         = flags( 6),
      negativeNextReadPos         = flags( 7),
      tooLargeNextReadPos         = flags( 8),
      tooFewBytesForReadName      = flags( 9),
      nonNullTerminatedReadName   = flags(10),
      nonASCIIReadName            = flags(11),
      noReadName                  = flags(12),
      emptyReadName               = flags(13),
      tooFewBytesForCigarOps      = flags(14),
      invalidCigarOp              = flags(15),
      tooFewRemainingBytesImplied = flags(16)
    )

  val TooFewFixedBlockBytes = fromBitSet(BitSet(0))
}
