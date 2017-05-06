package org.hammerlab.hadoop_bam

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.hammerlab.hadoop_bam.bgzf.VirtualPos
import org.seqdoop.hadoop_bam.FileVirtualSplit

sealed trait FileSplitResult

case class ExtendPrevious(path: Path, newEndPos: VirtualPos)
  extends FileSplitResult

case class VirtualSplit(path: Path,
                        start: VirtualPos,
                        end: VirtualPos,
                        locations: Array[String])
  extends InputSplit
    with FileSplitResult {

  override def getLength: Long =
    (end.blockPos - start.blockPos) match {
      case 0 ⇒ end.offset - start.offset
      case diff ⇒ diff * 0x10000
    }

  override def getLocations: Array[String] = locations
}

object VirtualSplit {
  implicit def apply(fvs: FileVirtualSplit): VirtualSplit =
    VirtualSplit(
      fvs.getPath,
      VirtualPos(fvs.getStartVirtualOffset),
      VirtualPos(fvs.getEndVirtualOffset),
      fvs.getLocations
    )
}
