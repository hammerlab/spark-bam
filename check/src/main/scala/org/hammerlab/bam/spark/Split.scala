package org.hammerlab.bam.spark

import hammerlab.show._
import org.hammerlab.bgzf.{ EstimatedCompressionRatio, Pos }
import org.hammerlab.kryo._
import org.seqdoop.hadoop_bam.FileVirtualSplit

case class Split(start: Pos,
                 end: Pos) {
  def length(implicit r: EstimatedCompressionRatio): Double =
    end - start
}

object Split {
  implicit def apply(t: (Pos, Pos)): Split = Split(t._1, t._2)
  implicit def apply(fvs: FileVirtualSplit): Split =
    Split(
      Pos(fvs.getStartVirtualOffset),
      Pos(fvs.getEndVirtualOffset)
    )

  implicit def makeShow(implicit showPos: Show[Pos]): Show[Split] =
    Show {
      case Split(start, end) ⇒
        show"$start-$end"
    }

  implicit val alsoRegister: AlsoRegister[Split] =
    AlsoRegister(
      cls[Pos]
    )
}
