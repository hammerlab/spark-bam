package org.hammerlab.hadoop_bam.bgzf

import java.util
import java.util.Comparator

import org.hammerlab.hadoop_bam.Index

import scala.collection.JavaConverters._

case class PosIndex(offsets: util.NavigableSet[Pos]) {
  def nextAlignment(filePos: Long): Pos =
    offsets.ceiling(Pos(filePos, 0))
}

object PosIndex {
  def apply(index: Index, bamSize: Long): PosIndex =
    new PosIndex(
      {
        val set = new util.TreeSet(implicitly[Comparator[Pos]])
        set.addAll(
          index
            .allAddresses
            .asJava
        )
        set.add(Pos(bamSize, 0))
        set
      }
    )
}
