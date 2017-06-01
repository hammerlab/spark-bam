package org.hammerlab.bgzf.hadoop

import org.apache.hadoop.mapreduce.InputSplit
import org.hammerlab.hadoop.Path

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
