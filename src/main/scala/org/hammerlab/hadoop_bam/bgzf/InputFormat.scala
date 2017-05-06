package org.hammerlab.hadoop_bam.bgzf

import java.io.IOException
import java.util

import math.min
import org.apache.hadoop.fs.{ FSDataInputStream, Path }
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat, FileSplit }
import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, RecordReader, TaskAttemptContext }
import org.hammerlab.hadoop_bam.bgzf.block.Block.{ MAX_BLOCK_SIZE, readHeader }
import org.hammerlab.hadoop_bam.bgzf.block.{ Block, Header, HeaderParseException, Stream }
import org.hammerlab.hadoop_bam.VirtualSplit

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

//class Writable(var block: Block)
//  extends org.apache.hadoop.io.Writable {
//
//  override def readFields(in: DataInput): Unit = {
//    val uncompressedSize = in.readInt()
//    val bytes = Array.fill[Byte](uncompressedSize)(0)
//    in.readFully(bytes)
//
//    block =
//      Block(
//        bytes,
//        in.readLong(),
//        in.readInt()
//      )
//  }
//
//  override def write(out: DataOutput): Unit = {
//    out.writeInt(block.bytes.length)
//    out.write(block.bytes)
//    out.writeLong(block.start)
//    out.writeInt(block.compressedSize)
//  }
//}

case class InputFormat(bgzfBlockHeadersToCheck: Int = 3)
  extends FileInputFormat[NullWritable, Block] {

  val maxBytesToRead =
    MAX_BLOCK_SIZE * bgzfBlockHeadersToCheck +
      (MAX_BLOCK_SIZE - 1)

  implicit val bytes = Array.fill[Byte](maxBytesToRead)(0)

  def nextBlockAlignment(path: Path,
                         start: Long,
                         end: Long,
                         in: FSDataInputStream): Long = {
    in.seek(start)

    val bytesRead = in.read(bytes)

    if (bytesRead != min(end - start, maxBytesToRead)) {
      throw new IOException(s"Unexpected failure to read $maxBytesToRead from [$start,$end) (expected ${end - start} available")
    }

    var pos = 0
    try {
      var curPos = pos
      for {
        _ ← 0 until bgzfBlockHeadersToCheck
      } {
        val Header(_, compressedSize) = readHeader(curPos, bytesRead)
        curPos += compressedSize
      }
    } catch {
      case _: HeaderParseException ⇒
        pos += 1
        if (pos == bytesRead) {
          throw HeaderSearchFailedException(
            path,
            start,
            maxBytesToRead
          )
        }
    }

    start + pos
  }

  override def getSplits(job: JobContext): util.List[InputSplit] = {
    val fileSplits =
      super
        .getSplits(job)
        .asScala
        .map(_.asInstanceOf[FileSplit])

    val conf = job.getConfiguration

    val streams = TrieMap[Path, (Long, FSDataInputStream)]()
    def stream(path: Path) =
      streams.getOrElseUpdate(
        path,
        {
          val fs = path.getFileSystem(conf)
          fs.getFileStatus(path).getLen →
            fs.open(path)
        }
      )

    (for {
      split ← fileSplits
      path = split.getPath
      (pathLength, in) = stream(path)
      start = split.getStart
      end = start + split.getLength
    } yield {
      val blockStart = nextBlockAlignment(path, start, end, in)
      val blockEnd = nextBlockAlignment(path, end, pathLength, in)

      new FileSplit(
        path,
        blockStart,
        blockEnd - blockStart,
        split.getLocations
      ): InputSplit
    })
    .asJava
  }

  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader[NullWritable, Block] = {
    val VirtualSplit(
      path,
      VirtualPos(startBlock, _),
      VirtualPos(endBlock, _),
      _
    ) =
      split.asInstanceOf[VirtualSplit]

    val length = endBlock - startBlock

    val fs = path.getFileSystem(context.getConfiguration)
    val is = fs.open(path)
    val iterator = Stream(is)

    new RecordReader[NullWritable, Block] {

      override def getCurrentKey: NullWritable = NullWritable.get()

      override def getProgress: Float =
        if (length == 0)
          0
        else
          (iterator.pos - startBlock) / length

      override def nextKeyValue(): Boolean = iterator.hasNext

      override def getCurrentValue: Block = iterator.head

      override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

      override def close(): Unit = is.close()
    }
  }
}

case class HeaderSearchFailedException(path: Path,
                                       start: Long,
                                       bytesRead: Int)
  extends IOException(
    s"$path: failed to find BGZF header in $bytesRead bytes from $start"
  )

