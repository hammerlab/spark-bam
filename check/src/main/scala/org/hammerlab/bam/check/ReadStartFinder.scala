package org.hammerlab.bam.check

import org.hammerlab.bgzf.Pos

trait ReadStartFinder
  extends Checker[Boolean] {
  def nextReadStart(start: Pos)(
      implicit
      maxReadSize: MaxReadSize
  ): Option[Pos]
}
