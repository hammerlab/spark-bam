package org.hammerlab.bgzf.hadoop

import org.apache.hadoop.mapreduce.InputSplit
import org.hammerlab.bgzf.block.Metadata
import org.hammerlab.hadoop.Path

case class BlocksSplit(path: Path,
                       start: Long,
                       length: Long,
                       end: Long,
                       blocks: Seq[Metadata],
                       locations: Array[String])
  extends InputSplit {
  override def getLength: Long = length
  override def getLocations: Array[String] = locations
}

object BlocksSplit {
  def apply(path: Path,
            blocks: Seq[Metadata],
            locations: Array[String]): BlocksSplit = {
    val start = blocks.head.start
    val last = blocks.last
    val end = last.start + last.compressedSize
    BlocksSplit(
      path,
      start,
      end - start,
      end,
      blocks,
      locations
    )
  }
}

case class Split(path: Path,
                 start: Long,
                 length: Long,
                 locations: Array[String],
                 blocksWhitelist: Option[Set[Long]] = None)
  extends InputSplit {

  override def getLength: Long = length
  override def getLocations: Array[String] = locations

  def end = start + start

  override def equals(obj: scala.Any): Boolean =
    obj match {
      case b: Split ⇒
          start == b.start &&
          length == b.length &&
          (locations sameElements b.locations) &&
          blocksWhitelist == b.blocksWhitelist
      case _ ⇒
        false
    }
}
