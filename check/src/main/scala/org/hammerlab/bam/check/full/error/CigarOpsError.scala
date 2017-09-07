package org.hammerlab.bam.check.full.error

/**
 * Container for flags related to inconsistencies in BAM-record-candidates' cigar-related fields.
 */
sealed trait CigarOpsError {
  def invalidCigarOp = false
  def tooFewBytesForCigarOps = false
  def emptyMappedCigar = false
  def emptyMappedSeq = false
}

case object InvalidCigarOp extends CigarOpsError {
  override def invalidCigarOp = true
}

case object TooFewBytesForCigarOps
  extends CigarOpsError {
  override def tooFewBytesForCigarOps = true
}

case class EmptyMapped(override val emptyMappedCigar: Boolean,
                       override val emptyMappedSeq: Boolean)
  extends CigarOpsError
