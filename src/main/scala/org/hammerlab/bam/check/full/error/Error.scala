package org.hammerlab.bam.check.full.error

/**
 * Bag of fields with information related to various inconsistencies in BAM-record-candidates.
 * @tparam T field type: [[Boolean]] for individual positions [[Flags]], [[Long]] for aggregate [[Counts]].
 */
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
  def emptyMappedCigar: T
  def emptyMappedSeq: T
  def tooFewRemainingBytesImplied: T
}
