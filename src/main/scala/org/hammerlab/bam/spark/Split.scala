package org.hammerlab.bam.spark

import org.hammerlab.bgzf.{ EstimatedCompressionRatio, Pos }
import org.seqdoop.hadoop_bam.FileVirtualSplit

case class Split(start: Pos,
                 end: Pos) {
  def length(implicit estimatedCompressionRatio: EstimatedCompressionRatio): Double =
    end - start
}

object Split {
  implicit def apply(t: (Pos, Pos)): Split = Split(t._1, t._2)
  implicit def apply(fvs: FileVirtualSplit): Split =
    Split(
      Pos(fvs.getStartVirtualOffset),
      Pos(fvs.getEndVirtualOffset)
    )
}
