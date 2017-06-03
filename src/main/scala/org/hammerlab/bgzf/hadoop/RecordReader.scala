package org.hammerlab.bgzf.hadoop

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.hammerlab.bgzf.block.{ Block, Metadata, Stream }
import org.hammerlab.hadoop.RecordReader
import org.hammerlab.io.SeekableByteChannel
import org.hammerlab.iterator.CloseableIterator
import org.hammerlab.iterator.SimpleBufferedIterator._

object RecordReader {

  implicit case object MetadataReader extends RecordReader[BlocksSplit, Long, Metadata] {
    override def records(split: BlocksSplit, ctx: TaskAttemptContext): CloseableIterator[(Long, Metadata)] =
      split
        .blocks
        .iterator
        .map(m ⇒ m.start → m)
        .buffer
  }

  implicit case object BlockReader extends RecordReader[Split, Long, Block] {
    override def records(split: Split, ctx: TaskAttemptContext): CloseableIterator[(Long, Block)] = {

      val Split(
        path,
        start,
        length,
        _,
        blocksWhitelistOpt
      ) =
        split.asInstanceOf[Split]

      val fs = path.getFileSystem(ctx.getConfiguration)

      val is: SeekableByteChannel = fs.open(path)
      is.seek(start)

      val blocks = Stream(is)

      val end = start + length

      blocksWhitelistOpt
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
  }
}
