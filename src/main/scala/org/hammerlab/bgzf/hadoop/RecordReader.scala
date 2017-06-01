package org.hammerlab.bgzf.hadoop

import org.apache.hadoop.mapreduce.{ InputSplit, TaskAttemptContext }
import org.hammerlab.bgzf.block.{ Block, Stream }
import org.hammerlab.io.SeekableByteChannel
import org.hammerlab.iterator.SimpleBufferedIterator
import org.hammerlab.iterator.SimpleBufferedIterator._

//case class RecordReader(is: InputStream,
//                        start: Long,
//                        length: Long,
//                        blocks: Stream,
//                        blocksWhitelist: Option[Set[Long]])
//  extends SimpleBufferedIterator[(Long, Block)] {
//
//}

object RecordReader {
  def apply(split: InputSplit,
            context: TaskAttemptContext): SimpleBufferedIterator[(Long, Block)] = {

    val Split(
      path,
      start,
      length,
      _,
      blocksWhitelistOpt
    ) =
      split.asInstanceOf[Split]

    val fs = path.getFileSystem(context.getConfiguration)

    val is: SeekableByteChannel = fs.open(path)
    is.seek(start)

    val blocks = Stream(is)

    RecordReader(
      start,
      start + length,
      blocks,
      blocksWhitelistOpt
    )
  }

  def apply(start: Long,
            end: Long,
            blocks: Stream,
            blocksWhitelist: Option[Set[Long]]): SimpleBufferedIterator[(Long, Block)] = {
    blocksWhitelist
      .map(
        whitelist ⇒
          blocks
            .filter(
              block ⇒
                whitelist.contains(block.start)
            )
            .buffered
      )
      .getOrElse(
        blocks
      )
      .map(
        block ⇒
          block.start →
            block
      )
      .takeWhile(_._1 < end)
      .buffer
  }

  implicit def make(split: InputSplit,
                    context: TaskAttemptContext): SimpleBufferedIterator[(Long, Block)] =
    apply(split, context)
}
