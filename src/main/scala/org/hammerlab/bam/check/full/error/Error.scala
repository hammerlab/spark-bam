package org.hammerlab.bam.check.full.error

trait Error[T] {
  def tooFewFixedBlockBytes: T
  def negativeReadIdx: T
  def tooLargeReadIdx: T
  def negativeReadPos: T
  def tooLargeReadPos: T
  def negativeNextReadIdx: T
  def tooLargeNextReadIdx: T
  def negativeNextReadPos: T
  def tooLargeNextReadPos: T
  def tooFewBytesForReadName: T
  def nonNullTerminatedReadName: T
  def nonASCIIReadName: T
  def noReadName: T
  def emptyReadName: T
  def tooFewBytesForCigarOps: T
  def invalidCigarOp: T
  def tooFewRemainingBytesImplied: T
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
