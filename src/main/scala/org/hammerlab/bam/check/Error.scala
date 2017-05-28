package org.hammerlab.bam.check

import shapeless._

case class ErrorT[T](tooFewFixedBlockBytes: T,
                     negativeReadIdx: T,
                     tooLargeReadIdx: T,
                     negativeReadPos: T,
                     tooLargeReadPos: T,
                     negativeNextReadIdx: T,
                     tooLargeNextReadIdx: T,
                     negativeNextReadPos: T,
                     tooLargeNextReadPos: T,
                     tooFewBytesForReadName: T,
                     nonNullTerminatedReadName: T,
                     nonASCIIReadName: T,
                     noReadName: T,
                     emptyReadName: T,
                     tooFewBytesForCigarOps: T,
                     invalidCigarOp: T,
                     tooFewBytesForSeqAndQuals: T)

object Error {

  type Flags = ErrorT[Boolean]
  type Counts = ErrorT[Long]

  /** Can't instantiate these inside the [[CountsWrapper]] [[AnyVal]] below. */
  private val labelledCounts = LabelledGeneric[Counts]
  private val genericCounts = Generic[Counts]

  implicit class CountsWrapper(val counts: Counts)
    extends AnyVal {

    import shapeless.record._

    /**
     * Convert an [[Counts]] to a values-descending array of (key,value) pairs
     */
    def descCounts: Array[(String, Long)] =
      labelledCounts
        .to(counts)
        .toMap
        .toArray
        .map {
          case (k, v) ⇒
            k.name → v
        }
        .sortBy(-_._2)

    /**
     * Pretty-print an [[Counts]]
     */
    def pp(indent: String = "",
           wrapFields: Boolean = false,
           includeZeros: Boolean = true): String = {

      val dc = descCounts

      val maxKeySize = dc.map(_._1.length).max
      val maxValSize = dc.map(_._2.toString.length).max

      val lines =
        for {
          (k, v) ← dc
          if (v > 0 || includeZeros)
        } yield
          s"${" " * (maxKeySize - k.length)}$k:\t${" " * (maxValSize - v.toString.length)}$v"

      if (wrapFields)
        lines
          .mkString(
            s"${indent}Errors(\n\t$indent",
            s"\n\t$indent",
            s"\n$indent)"
          )
      else
        lines
          .mkString(
            indent,
            s"\n$indent",
            ""
          )
    }

    /**
     * Count the number of non-zero fields in an [[Counts]]
     */
    def numNonZeroFields: Int =
      genericCounts
        .to(counts)
        .toList[Long]
        .count(_ > 0)
  }

  object toLong extends Poly1 {
    implicit val cs: Case.Aux[Boolean, Long] = at(b ⇒ if (b) 1 else 0)
  }

  /**
   * Convert an [[Flags]] to an [[Counts]] by changing [[true]]/[[false]] to [[1L]]/[[0L]]
   */
  implicit def toCounts(error: Flags): Counts =
    Generic[Counts]
      .from(
        Generic[Flags]
          .to(error)
          .map(toLong)
      )

  /**
   * Construct an [[Flags]] from some convenient wrappers around subsets of the possible flags
   */
  def apply(tooFewFixedBlockBytes: Boolean,
            readPosError: Option[RefPosError],
            nextReadPosError: Option[RefPosError],
            readNameError: Option[ReadNameError],
            cigarOpsError: Option[CigarOpsError],
            tooFewBytesForSeqAndQuals: Boolean): Flags =
    ErrorT(
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
      tooFewBytesForSeqAndQuals = tooFewBytesForSeqAndQuals
    )

  /**
   * Construct an [[Flags]] from some convenient, implicit wrappers around subsets of the possible flags
   */
  def apply(implicit
            posErrors: (Option[RefPosError], Option[RefPosError]),
            readNameError: Option[ReadNameError] = None,
            cigarOpsError: Option[CigarOpsError] = None,
            tooFewBytesForSeqAndQuals: Boolean = false): Option[Flags] =
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
