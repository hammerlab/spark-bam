package org.hammerlab.hadoop_bam

import java.io.IOException
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.{ List ⇒ JList }

import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FSDataInputStream, Path }
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.hammerlab.hadoop_bam.BAMInputFormat.NUM_GET_SPLITS_WORKERS_KEY
import org.seqdoop.hadoop_bam.util.WrapSeekable
import org.seqdoop.hadoop_bam.{ BAMSplitGuesser, FileVirtualSplit, BAMInputFormat ⇒ ParentFormat }

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

sealed trait FileSplitResult

case class ExtendPrevious(path: Path, newEndPos: VirtualPos)
  extends FileSplitResult

case class VirtualPos(blockPos: Long, offset: Int) {
  override def toString: String = s"$blockPos:$offset"
}

object VirtualPos {
  def apply(vpos: Long): VirtualPos = VirtualPos(vpos >> 16, (vpos & 0xffff).toInt)
}

case class VirtualSplit(path: Path,
                        start: VirtualPos,
                        end: VirtualPos,
                        locations: Array[String])
  extends InputSplit
    with FileSplitResult {

  override def getLength: Long =
    end.blockPos - start.blockPos match {
      case 0 ⇒ end.offset - start.offset
      case diff ⇒ diff * 0x10000
    }

  override def getLocations: Array[String] = locations
}

object VirtualSplit {
  implicit def apply(fvs: FileVirtualSplit): VirtualSplit =
    VirtualSplit(
      fvs.getPath,
      VirtualPos(fvs.getStartVirtualOffset),
      VirtualPos(fvs.getEndVirtualOffset),
      fvs.getLocations
    )
}

class BAMInputFormat
  extends ParentFormat
    with Logging {

  override def getSplits(splits: JList[InputSplit], cfg: Configuration): JList[InputSplit] = {

    /** Work queue, each [[FileSplit]] will be enqueued and then worker threads will pop them off and process them. */
    val queue = new ConcurrentLinkedDeque[(Int, FileSplit)]()

    for {
      (split, idx) ← splits.iterator().asScala.zipWithIndex
    } {
      queue.add(idx → split.asInstanceOf[FileSplit])
    }

    val numWorkers = cfg.getInt(NUM_GET_SPLITS_WORKERS_KEY, 32)

    /** Workers will drop a [[FileSplitResult]] for each [[FileSplit]] into this (thread-safe) map. */
    val fileSplitResultsMap = TrieMap[Int, FileSplitResult]()

    def pollQueue(): Unit = {

      val streams = mutable.Map[Path, WrapSeekable[FSDataInputStream]]()
      def stream(implicit path: Path): WrapSeekable[FSDataInputStream] =
        streams.getOrElseUpdate(
          path,
          WrapSeekable.openPath(path.getFileSystem(cfg), path)
        )

      val guessers = mutable.Map[WrapSeekable[FSDataInputStream], BAMSplitGuesser]()
      def guesser(implicit sin: WrapSeekable[FSDataInputStream]): BAMSplitGuesser =
        guessers.getOrElseUpdate(
          sin,
          new BAMSplitGuesser(sin, cfg)
        )

      while (true) {
        Option(queue.poll()) match {
          case Some((idx, split)) ⇒

            /**
             * The following logic is basically copied from [[org.seqdoop.hadoop_bam.BAMInputFormat]], but modified to
             * e.g. emit a [[FileSplitResult]].
             */

            implicit val path = split.getPath
            implicit val sin = stream

            val beg = split.getStart
            val end = beg + split.getLength

            logger.debug(s"Thread ${Thread.currentThread().getName} processing FileSplit $idx [$beg:$end)")

            // As the guesser goes to the next BGZF block before looking for BAM
            // records, the ending BGZF blocks have to always be traversed fully.
            // Hence force the length to be 0xffff, the maximum possible.
            val alignedEnd = VirtualPos(end, 0xffff)

            val result =
              guesser.guessNextBAMRecordStart(beg, end) match {
                case guess if guess == end ⇒
                  // No records detected in this split: merge it to the previous one.
                  // This could legitimately happen e.g. if we have a split that is
                  // so small that it only contains the middle part of a BGZF block.
                  //
                  // Of course, if it's the first split, then this is simply not a
                  // valid BAM file.
                  //
                  // FIXME: In theory, any number of splits could only contain parts
                  // of the BAM header before we start to see splits that contain BAM
                  // records. For now, we require that the split size is at least as
                  // big as the header and don't handle that case.
                  logger.debug(s"Split $idx: extend previous split to $alignedEnd")
                  ExtendPrevious(path, alignedEnd)
                case guess ⇒
                  val alignedBeg = VirtualPos(guess)
                  logger.debug(s"Split $idx: offset $alignedBeg")
                  VirtualSplit(path, alignedBeg, alignedEnd, split.getLocations)
              }

            fileSplitResultsMap(idx) = result
          case _ ⇒
            return
        }
      }
    }

    // Fork n-1 worker threads
    val threads =
      for {
        _ ← 0 until (numWorkers - 1)
      } yield {
        val thread =
          new Thread() {
            override def run(): Unit = {
              pollQueue()
            }
          }

        thread.start()
        thread
      }

    // Use this thread as the nth worker
    pollQueue()

    threads.foreach(_.join())

    val sortedResults =
      fileSplitResultsMap
        .toVector
        .sortBy(_._1)
        .map(_._2)

    val newSplits = ArrayBuffer[VirtualSplit]()
    var nextSplitOpt: Option[VirtualSplit] = None

    def flush(): Unit =
      nextSplitOpt match {
        case Some(nextSplit) ⇒
          newSplits += nextSplit
          nextSplitOpt = None
        case None ⇒
      }

    sortedResults foreach {
      case ExtendPrevious(path, newEndPos) ⇒
        nextSplitOpt match {
          case Some(nextSplit) ⇒
            nextSplitOpt = Some(nextSplit.copy(end = newEndPos))
          case None ⇒
            throw new IOException(
              s"$path: no reads in first split: bad BAM file or tiny split size?"
            )
        }
      case split: VirtualSplit ⇒
        flush()
        nextSplitOpt = Some(split)
    }

    flush()

    newSplits
      .map(split ⇒ split: InputSplit)
      .asJava
  }
}

object BAMInputFormat {
  val NUM_GET_SPLITS_WORKERS_KEY = "hadoop_bam.num_get_splits_workers"
}
