package org.hammerlab.hadoop_bam.bgzf.hadoop

import java.io.IOException
import java.util

import org.apache.hadoop.fs.{ FSDataInputStream, Path }
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.{ SPLIT_MAXSIZE, getMaxSplitSize }
import org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat, FileSplit }
import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, TaskAttemptContext }
import org.hammerlab.hadoop_bam.bgzf.block.Block.MAX_BLOCK_SIZE
import org.hammerlab.hadoop_bam.bgzf.block.{ Block, Header, HeaderParseException }

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer

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
                         in: FSDataInputStream): Long = {
    in.seek(start)

    val bytesRead = in.read(bytes)

    if (bytesRead != maxBytesToRead) {
      throw new IOException(s"Read $bytesRead bytes from $start; expected $maxBytesToRead")
    }

    var pos = 0
    while (pos < bytesRead) {
      try {
        var curPos = pos
        for {
          _ ← 0 until bgzfBlockHeadersToCheck
        } {
          val Header(_, compressedSize) = Header.make(curPos, bytesRead)
          curPos += compressedSize
        }

        return start + pos
      } catch {
        case _: HeaderParseException ⇒
          pos += 1
      }
    }

    throw HeaderSearchFailedException(path, start, bytesRead)
  }

  override def getSplits(job: JobContext): util.List[InputSplit] = {
    if (getMaxSplitSize(job) < maxBytesToRead) {
      job.getConfiguration.getLong(SPLIT_MAXSIZE, maxBytesToRead)
    }

    val originalFileSplits =
      super
        .getSplits(job)
        .asScala
        .map(_.asInstanceOf[FileSplit])

    val collapsedFileSplits = ArrayBuffer[FileSplit]()
    var nextSplitOpt: Option[FileSplit] = None

    def flush(): Unit =
      nextSplitOpt.foreach(
        nextSplit ⇒
          collapsedFileSplits += nextSplit
      )

    for {
      fileSplit ← originalFileSplits
      length = fileSplit.getLength
    } {
      if (length < maxBytesToRead) {
        nextSplitOpt match {
          case Some(nextSplit) if nextSplit.getPath == fileSplit.getPath ⇒
            nextSplitOpt =
              Some(
                new FileSplit(
                  fileSplit.getPath,
                  nextSplit.getStart,
                  nextSplit.getLength + length,
                  nextSplit.getLocations
                )
              )
          case _ ⇒
            flush()
            nextSplitOpt = Some(fileSplit)
        }
      }
    }

    flush()

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
      fileSplit ← collapsedFileSplits
      path = fileSplit.getPath
      (pathLength, in) = stream(path)
      start = fileSplit.getStart
      end = start + fileSplit.getLength
    } yield {
      if (start + maxBytesToRead > end) {
        throw new IOException(s"Bad split: [$start,$end) (length ${end - start}; minimum $maxBytesToRead")
      }
      val blockStart = nextBlockAlignment(path, start, in)
      val blockEnd =
        if (end < pathLength)
          nextBlockAlignment(path, end, in)
        else
          pathLength

      new FileSplit(
        path,
        blockStart,
        blockEnd - blockStart,
        fileSplit.getLocations
      ): InputSplit
    })
    .asJava
  }

  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader =
    RecordReader(split, context)
}

case class HeaderSearchFailedException(path: Path,
                                       start: Long,
                                       bytesRead: Int)
  extends IOException(
    s"$path: failed to find BGZF header in $bytesRead bytes from $start"
  )

