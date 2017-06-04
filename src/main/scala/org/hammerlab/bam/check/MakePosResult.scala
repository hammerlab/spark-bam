package org.hammerlab.bam.check

trait MakePosResult[Call, PosResult] {
  def apply(call: Call, isReadStart: Boolean): PosResult
}
