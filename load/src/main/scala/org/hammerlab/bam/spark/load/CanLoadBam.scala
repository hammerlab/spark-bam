package org.hammerlab.bam.spark.load

import grizzled.slf4j.Logging
import hammerlab.iterator._
import hammerlab.math.utils.ceil
import hammerlab.path._
import htsjdk.samtools.{ SAMLineParser, SAMRecord }
import org.apache.hadoop.fs
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check.{ MaxReadSize, ReadsToCheck }
import org.hammerlab.bam.header.ContigLengths.readSAMHeaderFromStream
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bam.iterator.RecordStream
import org.hammerlab.bam.spark.{ BAMRecordRDD, FindRecordStart, Split }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ BGZFBlocksToCheck, FindBlockStart }
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.{ FileSplits, MaxSplitSize }
import org.seqdoop.hadoop_bam.{ BAMRecordReader, CRAMInputFormat, FileVirtualSplit, SAMRecordWritable }
import shapeless.the

/**
 * Mix-in that exposes a `loadBam` and related methods on a [[SparkContext]], for loading [[SAMRecord]]s from a BAM
 * file.
 */
trait CanLoadBam
  extends Logging
     with Intervals {

  implicit def sc: SparkContext

  implicit def conf: Configuration = sc.hadoopConfiguration

  /**
   * Load an [[RDD]] of [[SAMRecord]]s from a SAM-file [[Path]].
   */
  def loadSam(path: Path,
              splitSize: MaxSplitSize = MaxSplitSize()): RDD[SAMRecord] = {
    val header =
      sc.broadcast(
        readSAMHeaderFromStream(
          path.inputStream,
          conf
        )
      )

    sc
      .textFile(
        path.toString,
        minPartitions =
          ceil[Long](
            path.size,
            splitSize
          )
          .toInt
      )
      .filter(!_.startsWith("@"))
      .mapPartitions {
        it ⇒
          val lineParser = new SAMLineParser(header.value)
          it.map(
            lineParser.parseLine
          )
      }
  }

  def loadBam(path: Path,
                      splitSize:      MaxSplitSize = MaxSplitSize(),
              bgzfBlocksToCheck: BGZFBlocksToCheck = the[BGZFBlocksToCheck],
                   readsToCheck:      ReadsToCheck = the[     ReadsToCheck],
                    maxReadSize:       MaxReadSize = the[      MaxReadSize]
             ): RDD[SAMRecord] =
    if (path.extension != "bam")
      throw new IllegalArgumentException(s"Expected 'bam' extension: $path")
    else {
      val contigLengthsBroadcast = sc.broadcast(ContigLengths(path))

      val fileSplitsRDD = SplitRDD(FileSplits.asJava(path, splitSize))

      val hconf = conf

      fileSplitsRDD
        .flatMap {
          case (start, end) ⇒
            implicit val Channels(
              compressedChannel,
              uncompressedBytes
            ) =
              Channels(path)

            val bgzfBlockStart =
              FindBlockStart(
                path,
                start,
                compressedChannel,
                bgzfBlocksToCheck
              )

            val startPos =
              FindRecordStart(
                path,
                bgzfBlockStart
              )(
                uncompressedBytes,
                contigLengthsBroadcast.value,
                readsToCheck,
                maxReadSize
              )

            val endPos = Pos(end, 0)

            /**
             * [[BAMRecordReader]] is much faster than [[RecordStream]] (possibly due to reusing the same JVM object for
             * each record); use it here since this is the most common method downstream libraries will consume, and no
             * additional metadata (e.g. read-start [[Pos]]) is needed,
             */
            val split =
              new FileVirtualSplit(
                new fs.Path(path.uri),
                startPos.toHTSJDK,
                endPos.toHTSJDK,
                Array()
              )

            val ctx = new TaskAttemptContextImpl(hconf, new TaskAttemptID)

            val rr = new BAMRecordReader
            rr.initialize(split, ctx)
            new SimpleIterator[SAMRecord] {
              override protected def _advance =
                if (rr.nextKeyValue())
                  Some(
                    rr.getCurrentValue.get()
                  )
                else
                  None
            }
        }
    }

  def loadSplitsAndReads(path: Path,
                         splitSize: MaxSplitSize = MaxSplitSize(),
                         bgzfBlocksToCheck: BGZFBlocksToCheck = the[BGZFBlocksToCheck],
                              readsToCheck:      ReadsToCheck = the[     ReadsToCheck],
                               maxReadSize:       MaxReadSize = the[      MaxReadSize]
                        ): BAMRecordRDD = {
    val positionsAndReadsRDD =
      loadReadsAndPositions(
        path,
        splitSize,
        bgzfBlocksToCheck,
        readsToCheck,
        maxReadSize
      )

    val endPos = Pos(path.size, 0)

    val splits =
      positionsAndReadsRDD
        .mapPartitions(
          it ⇒
            if (it.hasNext)
              Iterator(it.next._1)
            else
              Iterator()
        )
        .collect
        .sliding2(endPos)
        .map(Split(_))
        .toVector

    val reads = positionsAndReadsRDD.values

    BAMRecordRDD(splits, reads)
  }

  def loadReadsAndPositions(path: Path,
                            splitSize: MaxSplitSize = MaxSplitSize(),
                            bgzfBlocksToCheck: BGZFBlocksToCheck = the[BGZFBlocksToCheck],
                                 readsToCheck:      ReadsToCheck = the[     ReadsToCheck],
                                  maxReadSize:       MaxReadSize = the[      MaxReadSize]
                           ): RDD[(Pos, SAMRecord)] = {

    val headerBroadcast = sc.broadcast(Header(path))
    val contigLengthsBroadcast = sc.broadcast(ContigLengths(path))

    val fileSplitsRDD = SplitRDD(FileSplits.asJava(path, splitSize))

    fileSplitsRDD
      .flatMap {
        case (start, end) ⇒
          implicit val Channels(
            compressedChannel,
            uncompressedBytes
          ) =
            Channels(path)

          val bgzfBlockStart =
            FindBlockStart(
              path,
              start,
              compressedChannel,
              bgzfBlocksToCheck
            )

          val header = headerBroadcast.value

          val startPos =
            FindRecordStart(
              path,
              bgzfBlockStart
            )(
              uncompressedBytes,
              contigLengthsBroadcast.value,
              readsToCheck,
              maxReadSize
            )

          val rs = RecordStream(uncompressedBytes, header)

          uncompressedBytes.seek(startPos)

          val endPos = Pos(end, 0)

          rs
            .takeWhile {
              case (pos, _) ⇒
                pos < endPos
            }
      }
  }

  /**
   * Load reads from a .sam, .bam, or .cram file
   *
   * @param path Path to a .sam or .bam file
   * @param bgzfBlocksToCheck when searching for bgzf-block-boundaries, check this many blocks ahead before declaring a
   *                          position to be a block-boundary
   * @param readsToCheck number of successive reads to verify before emitting a record-/split-boundary
   * @param maxReadSize when searching for BAM-record-boundaries, try up to this many consecutive positions before
   *                    giving up / throwing; reads taking up more than this many bytes on disk can result in
   *                    "false-negative" read-boundary calls
   * @param splitSize maximum (compressed) size of generated partitions
   */
  def loadReads(path: Path,
                bgzfBlocksToCheck: BGZFBlocksToCheck = the[BGZFBlocksToCheck],
                     readsToCheck:      ReadsToCheck = the[     ReadsToCheck],
                      maxReadSize:       MaxReadSize = the[      MaxReadSize],
                splitSize: MaxSplitSize = MaxSplitSize()): RDD[SAMRecord] =
    path.extension match {
      case "sam" ⇒
        loadSam(
          path,
          splitSize
        )
      case "bam" ⇒
        loadBam(
          path,
          splitSize,
          bgzfBlocksToCheck,
          readsToCheck,
          maxReadSize
        )
      case "cram" ⇒
        // Delegate to hadoop-bam
        sc
          .newAPIHadoopFile(
            path.toString,
            classOf[CRAMInputFormat],
            classOf[LongWritable],
            classOf[SAMRecordWritable]
          )
          .values
          .map(_.get)
      case _ ⇒
        throw new IllegalArgumentException(
          s"Can't load reads from path: $path"
        )
    }
}
