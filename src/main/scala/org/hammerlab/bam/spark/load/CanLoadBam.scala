package org.hammerlab.bam.spark.load

import grizzled.slf4j.Logging
import htsjdk.samtools.BAMFileReader.getFileSpan
import htsjdk.samtools.SamReaderFactory.Option._
import htsjdk.samtools.{ QueryInterval, SAMLineParser, SAMRecord, SamReaderFactory }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.bam.iterator.SeekableRecordStream
import org.hammerlab.bam.spark.{ BAMRecordRDD, FindRecordStart, ParallelConfig, Split, Threads }
import org.hammerlab.bgzf.block.{ FindBlockStart, SeekableUncompressedBytes }
import org.hammerlab.bgzf.{ EstimatedCompressionRatio, Pos }
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.{ Locus, Region }
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.{ FileSplits, MaxSplitSize }
import org.hammerlab.io.CachingChannel._
import org.hammerlab.io.SeekableByteChannel
import org.hammerlab.iterator.CappedCostGroupsIterator.ElementTooCostlyStrategy.EmitAlone
import org.hammerlab.iterator.CappedCostGroupsIterator._
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.iterator.SimpleBufferedIterator
import org.hammerlab.iterator.sliding.Sliding2Iterator._
import org.hammerlab.math.ceil
import org.hammerlab.parallel._
import org.hammerlab.paths.Path
import org.hammerlab.{ hadoop, parallel }
import org.seqdoop.hadoop_bam.util.SAMHeaderReader.readSAMHeaderFromStream

import scala.collection.JavaConverters._

/**
 * Add a `loadBam` method to [[SparkContext]] for loading [[SAMRecord]]s from a BAM file.
 */
object CanLoadBam {

  def getIntevalChunks(path: Path,
                       intervals: LociSet)(
      implicit
      conf: Configuration
  ): Seq[Chunk] = {

    val readerFactory =
      SamReaderFactory
        .makeDefault
        .setOption(CACHE_FILE_BASED_INDEXES, true)
        .setOption(EAGERLY_DECODE, false)
        .setUseAsyncIo(false)

    val samReader = readerFactory.open(path)

    val header = readSAMHeaderFromStream(path.inputStream, conf)
    val dict = header.getSequenceDictionary
    val idx = samReader.indexing.getIndex
    val queryIntervals =
      for {
        interval ← intervals.toHtsJDKIntervals.toArray
      } yield
        new QueryInterval(
          dict.getSequenceIndex(interval.getContig),
          interval.getStart,
          interval.getEnd
        )

    val span = getFileSpan(queryIntervals, idx)

    span
      .getChunks
      .asScala
      .map(x ⇒ x: Chunk)
  }

  def region(record: SAMRecord): Option[Region] =
    Option(record.getContig).map(
      contig ⇒
        Region(
          contig,
          Locus(record.getStart - 1),
          Locus(record.getEnd)
        )
    )
}

trait CanLoadBam
  extends Logging {

  import CanLoadBam._

  implicit def sc: SparkContext

  implicit def conf: Configuration = sc.hadoopConfiguration

  def loadBamIntervals(path: Path,
                       intervals: LociSet,
                       splitSize: MaxSplitSize = MaxSplitSize(),
                       estimatedCompressionRatio: EstimatedCompressionRatio = 3.0): RDD[SAMRecord] = {

    val intervalsBroadcast = sc.broadcast(intervals)

    if (path.toString.endsWith(".sam")) {
      warn(s"Attempting to load SAM file $path with intervals filter")
      return loadSam(path, splitSize)
             .filter {
               record ⇒
                 region(record).exists(
                   intervalsBroadcast
                     .value
                     .intersects
                 )
             }
    }

    val chunks = getIntevalChunks(path, intervals)

    val chunkPartitions =
      chunks
        .iterator
        .cappedCostGroups(
          _.size(estimatedCompressionRatio),
          splitSize.toDouble
        )
        .map(_.toVector)
        .toVector

    sc
      .parallelize(
        chunkPartitions,
        chunkPartitions.size
      )
      .flatMap(chunks ⇒ chunks)
      .mapPartitions {
        chunks ⇒

          val compressedChannel = SeekableByteChannel(path).cache

          val uncompressedBytes = SeekableUncompressedBytes(compressedChannel)

          val records = SeekableRecordStream(uncompressedBytes)

          chunks
            .flatMap {
              chunk ⇒
                records.seek(chunk.start)

                new SimpleBufferedIterator[SAMRecord] {
                  override protected def _advance: Option[SAMRecord] =
                    if (records.hasNext) {
                      val (pos, record) = records.next
                      if (pos >= chunk.end)
                        None
                      else if (
                        region(record).exists(
                          intervalsBroadcast
                            .value
                            .intersects
                        )
                      )
                        Some(record)
                      else
                        _advance
                    } else
                      None
                }
          }
          .finish(
            records.close()
          )
      }
  }

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
              parallelConfig: ParallelConfig = Threads(),
              bgzfBlockHeadersToCheck: Int = 5,
              maxReadSize: Int = 10000000,
              splitSize: MaxSplitSize = MaxSplitSize()
              ): BAMRecordRDD = {

    val fileSplitStarts =
      FileSplits(
        hadoop.Path(path.uri),
        splitSize
      )
      .map(_.start)

    val endPos = Pos(path.size, 0)

    val contigLengthsBroadcast = sc.broadcast(ContigLengths(path))

    implicit val parallelizer: parallel.Config = parallelConfig

    val splits =
      fileSplitStarts
        .pmap {
          fileSplitStart ⇒

            val compressedChannel =
              SeekableByteChannel(path).cache

            val bgzfBlockStart =
              FindBlockStart(
                path,
                fileSplitStart,
                compressedChannel,
                bgzfBlockHeadersToCheck
              )

            val uncompressedBytes = SeekableUncompressedBytes(compressedChannel)

            val bamRecordStart =
              FindRecordStart(
                path,
                uncompressedBytes,
                bgzfBlockStart,
                contigLengthsBroadcast.value,
                maxReadSize
              )

            uncompressedBytes.close()

            bamRecordStart
        }
        .sliding2(endPos)
        .filter {
          case (start, end) ⇒
            end > start
        }
        .map(Split(_))
        .toVector

    val splitsRDD =
      sc.parallelize(
        splits,
        splits.size
      )

    val readsRDD =
      splitsRDD
        .flatMap {
          case Split(start, end) ⇒

            val uncompressedBytes =
              SeekableUncompressedBytes(
                SeekableByteChannel(path).cache
              )

            val recordStream = SeekableRecordStream(uncompressedBytes)
            recordStream.seek(start)

            new SimpleBufferedIterator[SAMRecord] {
              override protected def _advance: Option[SAMRecord] =
                recordStream
                  .nextOption
                    .flatMap {
                      case (pos, read) ⇒
                        if (pos < end)
                          Some(read)
                        else
                          None
                    }

              override protected def done(): Unit =
                recordStream.close()
            }
        }

    BAMRecordRDD(splits, readsRDD)
  }

  /**
   * Load reads from a .sam or .bam file
   *
   * @param path Path to a .sam or .bam file
   * @param bgzfBlockHeadersToCheck when searching for bgzf-block-boundaries, check this many blocks ahead before
   *                                declaring a position to be a block-boundary
   * @param maxReadSize when searching for BAM-record-boundaries, try up to this many consecutive positions before
   *                    giving up / throwing; reads taking up more than this many bytes on disk can result in
   *                    "false-negative" read-boundary calls
   * @param parallelConfig configure computing splits with [[Threads]] or [[org.hammerlab.bam.spark.Spark]]
   * @param splitSize maximum split size to pass to [[org.apache.hadoop.mapreduce.lib.input.FileInputFormat]].
   * @param estimatedCompressionRatio used to estimate distances between [[Pos]]s / sizes of [[Chunk]]s
   */
  def loadReads(path: Path,
                parallelConfig: ParallelConfig = Threads(),
                bgzfBlockHeadersToCheck: Int = 5,
                maxReadSize: Int = 10000000,
                splitSize: MaxSplitSize = MaxSplitSize(),
                estimatedCompressionRatio: EstimatedCompressionRatio = 3.0): RDD[SAMRecord] =
    path.extension match {
      case "sam" ⇒
        loadSam(
          path,
          splitSize
        )
      case "bam" ⇒
        loadBam(
          path,
          parallelConfig,
          bgzfBlockHeadersToCheck,
          maxReadSize,
          splitSize
        )
      case _ ⇒
        throw new IllegalArgumentException(
          s"Can't load reads from path: $path"
        )
    }
}
