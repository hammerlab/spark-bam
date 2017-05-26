package org.hammerlab.hadoop_bam.bgzf.hadoop

import java.io.{ DataInput, DataOutput }
import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ Text, Writable }
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat, FileSplit }
import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, TaskAttemptContext }
import org.hammerlab.hadoop_bam.bgzf.hadoop.BytesInputFormat.RANGES_KEY
import org.hammerlab.iterator.SimpleBufferedIterator
import org.hammerlab.iterator.Sliding2Iterator._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class UncompressedBlock(bytes: Array[Byte])

class BytesSplit
  extends InputSplit
    with Writable {

  var path: Path = _
  var blocks: Seq[Long] = _
  var lastBlockEnd: Long = _
  var getLocations: Array[String] = _

  override def getLength: Long = lastBlockEnd - blocks.head
  def ranges: BufferedIterator[(Long, Long)] =
    (blocks :+ lastBlockEnd)
      .sliding2
      .buffered

  override def write(out: DataOutput): Unit = {
    Text.writeString(out, path.toString)
    out.writeInt(blocks.size)
    blocks.foreach(out.writeLong)
    out.writeLong(lastBlockEnd)
  }

  override def readFields(in: DataInput): Unit = {
    path = new Path(Text.readString(in))
    val numBlocks = in.readInt()
    val blockBuf = ArrayBuffer[Long]()
    for { _ ← 0 until numBlocks } {
      blockBuf += in.readLong()
    }
    blocks = blockBuf
    lastBlockEnd = in.readLong()
    getLocations = null
  }
}

object BytesSplit {
  def apply(path: Path,
            blocks: Seq[Long],
            lastBlockEnd: Long,
            getLocations: Array[String]): BytesSplit = {
    val bs = new BytesSplit
    bs.path = path
    bs.blocks = blocks
    bs.lastBlockEnd = lastBlockEnd
    bs.getLocations = getLocations
    bs
  }
}

class BytesInputFormat
  extends FileInputFormat[Long, UncompressedBlock] {

  override def getSplits(job: JobContext): util.List[InputSplit] = {

    val fileSplits =
      super
        .getSplits(job)
        .asScala
        .map(_.asInstanceOf[FileSplit])

    val path = fileSplits.head.getPath

    val conf = job.getConfiguration

    val fs = path.getFileSystem(conf)

    val ranges =
      job
        .getConfiguration
        .get(RANGES_KEY)
        .split(",")
        .map(_.toLong)

    val slidingRanges =
      ranges
        .sliding2
        .buffered

    fileSplits
      .flatMap {
        fileSplit ⇒
          val end = fileSplit.getStart + fileSplit.getLength
          val blocks = ArrayBuffer[Long]()
          var lastEndOpt: Option[Long] = None
          while (slidingRanges.hasNext && slidingRanges.head._1 <= end) {
            val (offset, nextOffset) = slidingRanges.next
            blocks += offset
            lastEndOpt = Some(nextOffset)
          }
          lastEndOpt.map(
            lastEnd ⇒
              BytesSplit(
                fileSplit.getPath,
                blocks,
                lastEnd,
                fileSplit.getLocations
              ): InputSplit
          )
      }
      .asJava
  }

  override def createRecordReader(splt: InputSplit,
                                  context: TaskAttemptContext): mapreduce.RecordReader[Long, UncompressedBlock] =
    new mapreduce.RecordReader[Long, UncompressedBlock] {
      val split = splt.asInstanceOf[BytesSplit]

      val path = split.path
      val fs = path.getFileSystem(context.getConfiguration)

      val is = fs.open(path)

      val blocksIter = split.ranges

      var idx = 0
      val it =
        new SimpleBufferedIterator[(Long, UncompressedBlock)] {
          override protected def _advance: Option[(Long, UncompressedBlock)] = {
            if (blocksIter.hasNext) {
              val (start, end) = blocksIter.next
              is.seek(start)
              val bytes = Array.fill[Byte]((end - start).toInt)(0)
              is.readFully(bytes)
              Some(start, UncompressedBlock(bytes))
            } else
              None
          }
        }

      var hasNext = true

      override def getCurrentKey: Long = it.head._1

      override def getProgress: Float = if (hasNext) 0 else 1

      var first = true
      override def nextKeyValue(): Boolean =
        if (first) {
          first = false
          it.hasNext
        } else {
          it.next
          it.hasNext
        }

      override def getCurrentValue: UncompressedBlock = it.head._2
      override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}
      override def close(): Unit = {
        is.close()
      }
    }
}

object BytesInputFormat {
  val RANGES_KEY = "BytesInputFormat.ranges"
}
