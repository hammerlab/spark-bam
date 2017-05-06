package org.hammerlab.hadoop_bam.bgzf

import java.util
import java.util.Comparator

import org.hammerlab.hadoop_bam.Index

import scala.collection.JavaConverters._

case class VirtualPosIndex(offsets: util.NavigableSet[VirtualPos]) {
  def nextAlignment(filePos: Long): VirtualPos =
    offsets.ceiling(VirtualPos(filePos, 0))
}

object VirtualPosIndex {
  def apply(index: Index, bamSize: Long): VirtualPosIndex =
    new VirtualPosIndex(
      {
        val set = new util.TreeSet(implicitly[Comparator[VirtualPos]])
        set.addAll(
          index
            .allAddresses
            .asJava
        )
        set.add(VirtualPos(bamSize, 0))
        set
      }
    )
}
