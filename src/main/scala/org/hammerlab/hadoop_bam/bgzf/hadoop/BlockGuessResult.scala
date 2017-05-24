package org.hammerlab.hadoop_bam.bgzf.hadoop

sealed trait BlockGuessResult

case class BlockPos(pos: Long) extends BlockGuessResult
case class ExtendPrevious(end: Long) extends BlockGuessResult
