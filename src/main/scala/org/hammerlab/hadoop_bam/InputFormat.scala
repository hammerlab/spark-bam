package org.hammerlab.hadoop_bam

import java.io.IOException
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.{ List ⇒ JList }

import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FSDataInputStream, Path }
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.hammerlab.hadoop_bam.InputFormat.NUM_GET_SPLITS_WORKERS_KEY
import org.hammerlab.hadoop_bam.bgzf.{ VirtualPos, VirtualPosIndex }
import org.seqdoop.hadoop_bam.util.WrapSeekable
import org.seqdoop.hadoop_bam.{ BAMSplitGuesser, FileVirtualSplit, BAMInputFormat ⇒ ParentFormat }

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class InputFormat
  extends ParentFormat
    with Logging {

  override def getSplits(splits: JList[InputSplit], cfg: Configuration): JList[InputSplit] =
    getSplits(
      splits
        .asScala
        .map(_.asInstanceOf[FileSplit]),
      cfg
    )

  type PathInputStream = WrapSeekable[FSDataInputStream]

  def getSplits(fileSplits: Seq[FileSplit], cfg: Configuration): JList[InputSplit] = {

    val fileSplitsByPath = fileSplits.zipWithIndex.map(_.swap).groupBy(_._2.getPath)

    /** [[FileSplitResult]]s will be placed here for each [[FileSplit]] */
    val fileSplitResultsMap = TrieMap[Int, FileSplitResult]()

    /** Work queue, each [[FileSplit]] will be enqueued and then worker threads will pop them off and process them. */
    val queue = new ConcurrentLinkedDeque[(Int, FileSplit)]()

    val pathIndexMap = TrieMap[Path, VirtualPosIndex]()

    for {
      (path, splits) ← fileSplitsByPath
      fs = path.getFileSystem(cfg)
    } {
      queue.addAll(splits.asJava)

      /** If an index exists for [[path]], pull all [[VirtualPos]] addresses from it and cache them. */
      val baiPath = path.suffix(".bai")
      if (fs.exists(baiPath)) {
        val index = Index(baiPath)
        val length = cfg.getLong("file.length.override", fs.getFileStatus(path).getLen)
        pathIndexMap(path) = VirtualPosIndex(index, length)
      }
    }

    if (!queue.isEmpty) {
      val numWorkers = cfg.getInt(NUM_GET_SPLITS_WORKERS_KEY, 32)

      /**
       * Each thread runs this function, looping until [[queue]] is empty, processing a split each time, and keeping a
       * cache of [[WrapSeekable]]s and corresponding [[BAMSplitGuesser]]s.
       */
      def pollQueue(): Unit = {

        /** Cache a stream for each [[Path]]. */
        val streams = mutable.Map[Path, PathInputStream]()

        def stream(implicit path: Path): PathInputStream =
          streams.getOrElseUpdate(
            path,
            WrapSeekable.openPath(path.getFileSystem(cfg), path)
          )

        /** Cache [[BAMSplitGuesser]]s for each input-stream. */
        val guessers = mutable.Map[PathInputStream, BAMSplitGuesser]()

        def guesser(implicit in: PathInputStream): BAMSplitGuesser =
          guessers.getOrElseUpdate(
            in,
            new BAMSplitGuesser(in, cfg)
          )

        while (true) {
          Option(queue.poll()) match {
            case Some((idx, split)) ⇒

              /**
               * The following logic is basically copied from [[org.seqdoop.hadoop_bam.BAMInputFormat]], but modified to
               * e.g. emit a [[FileSplitResult]].
               */

              implicit val path = split.getPath
              implicit val in = stream

              val start = split.getStart
              val end = start + split.getLength

              logger.debug(s"Thread ${Thread.currentThread().getName} processing FileSplit $idx [$start:$end)")

              val result =
                pathIndexMap.get(path) match {

                  // Use an index, if present
                  case Some(index) ⇒
                    val virtualStart = index.nextAlignment(start)
                    val virtualEnd =
                      Option(index.nextAlignment(end)) match {
                        case Some(pos) ⇒ pos
                        case None ⇒
                          throw new IllegalArgumentException(
                            s"Couldn't find virtualEnd position for $path from $end"
                          )
                      }

                    logger.debug(s"Split $idx from index: $virtualStart-$virtualEnd")
                    VirtualSplit(path, virtualStart, virtualEnd, split.getLocations)

                  // Otherwise, find positions with the "guesser"
                  case None ⇒

                    // As the guesser goes to the next BGZF block before looking for BAM
                    // records, the ending BGZF blocks have to always be traversed fully.
                    // Hence force the length to be 0xffff, the maximum possible.
                    val virtualEnd = VirtualPos(end, 0xffff)

                    guesser.guessNextBAMRecordStart(start, end) match {
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
                        logger.debug(s"Split $idx: extend previous split to $virtualEnd")
                        ExtendPrevious(path, virtualEnd)
                      case guess ⇒
                        val virtualStart = VirtualPos(guess)
                        logger.debug(s"Split $idx: $virtualStart-$virtualEnd")
                        VirtualSplit(path, virtualStart, virtualEnd, split.getLocations)
                    }
                }

              fileSplitResultsMap(idx) = result
            case _ ⇒
              return
          }
        }
      }

      // Fork n-1 worker threads; the current thread will be used as the nth one.
      val threads =
        for {
          _ ← 0 until (numWorkers - 1)
        } yield {
          val thread =
            new Thread() {
              override def run(): Unit = pollQueue()
            }

          thread.start()
          thread
        }

      // Use this thread as the nth worker
      pollQueue()

      threads.foreach(_.join())
    }

    val sortedResults =
      fileSplitResultsMap
        .toVector
        .sortBy(_._1)
        .map(_._2)

    /**
     * Each [[FileSplitResult]] is either a [[VirtualSplit]] or an instance of [[ExtendPrevious]]; runs of the latter
     * should be folded in to the nearest-preceding [[FileVirtualSplit]].
     */

    // Buffer for accumulating finished splits
    val newSplits = ArrayBuffer[VirtualSplit]()

    /**
     * Staging buffer for the "next" split, which may be extended by [[ExtendPrevious]]s before the split is
     * "finished".
     */
    var nextSplitOpt: Option[VirtualSplit] = None

    /**
     * If there is a split in [[nextSplitOpt]] and it is known to be finished, append it to [[newSplits]] and clear
     * [[nextSplitOpt]].
     */
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
            // Extend the current "in-progress" split
            nextSplitOpt = Some(nextSplit.copy(end = newEndPos))
          case None ⇒
            throw new IOException(
              s"$path: no reads in first split: bad BAM file or tiny split size?"
            )
        }
      case split: VirtualSplit ⇒
        // Any previous in-progress split can be considered finished.
        flush()

        /** Keep this split around for merging with succeeding [[ExtendPrevious]]s. */
        nextSplitOpt = Some(split)
    }

    flush()

    newSplits
      .asInstanceOf[Seq[InputSplit]]  // Have to up-cast explicitly because Java doesn't have variance.
      .asJava
  }
}

object InputFormat {
  val NUM_GET_SPLITS_WORKERS_KEY = "hadoop_bam.num_get_splits_workers"
}
