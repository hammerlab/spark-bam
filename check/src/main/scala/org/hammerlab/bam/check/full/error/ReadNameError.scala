package org.hammerlab.bam.check.full.error

/**
 * Container for flags related to inconsistencies in BAM-record-candidates' read-name-length and read-name fields.
 */
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
