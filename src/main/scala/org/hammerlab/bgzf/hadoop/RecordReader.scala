package org.hammerlab.bgzf.hadoop

import java.io.InputStream

import org.apache.hadoop.mapreduce
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{ InputSplit, TaskAttemptContext }
import org.hammerlab.bam.split.Split
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ Block, Stream }

case class RecordReader(is: InputStream,
                        startBlock: Long,
                        iterator: Stream,
                        length: Long)
  extends mapreduce.RecordReader[NullWritable, Block] {

  override def getCurrentKey: NullWritable = NullWritable.get()

  override def getProgress: Float =
    if (length == 0)
      0
    else
      (iterator.blockStart - startBlock) / length

  override def nextKeyValue(): Boolean = iterator.hasNext

  override def getCurrentValue: Block = iterator.head

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

  override def close(): Unit = is.close()
}

object RecordReader {
  def apply(split: InputSplit,
            context: TaskAttemptContext): RecordReader = {
    val Split(
      path,
      Pos(startBlock, _),
      Pos(endBlock, _),
      _
    ) =
      split.asInstanceOf[Split]

    val length = endBlock - startBlock

    val fs = path.getFileSystem(context.getConfiguration)
    val is = fs.open(path)
    val iterator = Stream(is)

    RecordReader(
      is,
      startBlock,
      iterator,
      length
    )
  }
}
