package org.hammerlab.bam.check.full.error

import cats.Show
import cats.Show.show
import shapeless.ops.hlist.{ Length, Mapper }

import scala.collection.immutable.BitSet

sealed trait Result {
  def call: Boolean
}

case class Success(readsParsed: Int) extends Result {
  override def call: Boolean = true
}

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
                 tooFewRemainingBytesImplied: Boolean,
                 readsBeforeError: Int
                )
  extends Error[Boolean]
    with Result {
  override def call: Boolean = false
}

object Flags {

  import shapeless._
  import ops.record._

  object toCount extends Poly1 {
    implicit val flagCase: Case.Aux[Boolean, Long] =
      at(
        b ⇒
          if (b)
            1
          else
            0
      )

    implicit val readsBeforeErrorCase: Case.Aux[Int, Map[Int, Long]] =
      at(
        readsBeforeError ⇒
          if (readsBeforeError > 0)
            Map(readsBeforeError → 1L)
          else
            Map.empty
      )
  }

  val gen = Generic[Flags]

  val size: Int = Length[gen.Repr].apply().toInt

  object nonZeroCountField extends Poly1 {
    implicit val flagCase: Case.Aux[Boolean, Boolean] =
      at(b ⇒ b)

    implicit val readsBeforeErrorCase: Case.Aux[Int, Boolean] =
      at(_ > 0)
  }

  object boolFields extends Poly1 {
    implicit val flagCase: Case.Aux[Boolean, Boolean] =
      at(b ⇒ b)
  }

  implicit class FlagsWrapper(val flags: Flags) extends AnyVal {

    /**
     * Convert an [[Flags]] to an [[Counts]] by changing true/false to [[1L]]/[[0L]]
     */
    implicit def toCounts: Counts =
      /*Counts
        .gen
        .from(
          gen
            .to(flags)
            .map(toCount)
        )*/
      ???

//    implicitly[Mapper[nonZeroCountField.type, gen.Repr]]
//    the[Mapper[nonZeroCountField.type, gen.Repr]]

    /**
     * Count the number of non-zero fields in an [[Counts]]
     */
    def numNonZeroFields: Int =
      /*gen
        .to(flags)
        .map(nonZeroCountField)
        .toList[Boolean]
        .count(x ⇒ x)*/
    ???

    def trueFields: List[String] = {
      /*val lgf = lg.to(flags)

      val keys =
        Keys[lg.Repr]
          .apply()
          .toList[Symbol]
          .map(_.name)

      val values =
        Values[lg.Repr]
          .apply(lgf)
          .map(nonZeroCountField)
          .toList[Boolean]

      keys
        .zip(values)
        .filter(_._2)
        .map(_._1)*/
      ???
    }
  }

  private val lg = LabelledGeneric[Flags]

  implicit def makeShow: Show[Flags] =
    show {
      _
        .trueFields
        .mkString(",")
    }

  /**
   * Construct an [[Flags]] from some convenient wrappers around subsets of the possible flags
   */
  def apply(tooFewFixedBlockBytes: Boolean,
            readPosError: Option[RefPosError],
            nextReadPosError: Option[RefPosError],
            readNameError: Option[ReadNameError],
            cigarOpsError: Option[CigarOpsError],
            tooFewRemainingBytesImplied: Boolean,
            readsBeforeError: Int): Flags =
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
      tooFewRemainingBytesImplied = tooFewRemainingBytesImplied,

      readsBeforeError = readsBeforeError
    )

  /** Convert to and from a [[BitSet]] during serialization. */
  implicit def toBitSet(flags: Flags): (BitSet, Int) =
    /*BitSet(
      Generic[Flags]
        .to(flags)
        .collect(boolFields)
        .toList[Boolean]
        .zipWithIndex
        .flatMap {
          case (flag, idx) ⇒
            if (flag)
              Some(idx)
            else
              None
        }: _*
    ) →
      flags.readsBeforeError*/ ???

  implicit def fromBitSet(flags: (BitSet, Int)): Flags =
    Flags(
      tooFewFixedBlockBytes       = flags._1( 0),
      negativeReadIdx             = flags._1( 1),
      tooLargeReadIdx             = flags._1( 2),
      negativeReadPos             = flags._1( 3),
      tooLargeReadPos             = flags._1( 4),
      negativeNextReadIdx         = flags._1( 5),
      tooLargeNextReadIdx         = flags._1( 6),
      negativeNextReadPos         = flags._1( 7),
      tooLargeNextReadPos         = flags._1( 8),
      tooFewBytesForReadName      = flags._1( 9),
      nonNullTerminatedReadName   = flags._1(10),
      nonASCIIReadName            = flags._1(11),
      noReadName                  = flags._1(12),
      emptyReadName               = flags._1(13),
      tooFewBytesForCigarOps      = flags._1(14),
      invalidCigarOp              = flags._1(15),
      tooFewRemainingBytesImplied = flags._1(16),
      readsBeforeError            = flags._2
    )

  val TooFewFixedBlockBytes = fromBitSet(BitSet(0) → 0)
}
