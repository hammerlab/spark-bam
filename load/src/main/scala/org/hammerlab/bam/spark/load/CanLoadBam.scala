package org.hammerlab.bam.spark.load

import grizzled.slf4j.Logging
import hammerlab.iterator._
import hammerlab.path._
import htsjdk.samtools.BAMFileReader.getFileSpan
import htsjdk.samtools.SamReaderFactory.Option._
import htsjdk.samtools.{ QueryInterval, SAMLineParser, SAMRecord, SamReaderFactory }
import org.apache.hadoop.fs
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check.Checker.default
import org.hammerlab.bam.check.{ MaxReadSize, ReadsToCheck }
import org.hammerlab.bam.header.ContigLengths.readSAMHeaderFromStream
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.bam.iterator.{ RecordStream, SeekableRecordStream }
import org.hammerlab.bam.spark.{ BAMRecordRDD, FindRecordStart, Split }
import org.hammerlab.bgzf.block.{ BGZFBlocksToCheck, FindBlockStart, SeekableUncompressedBytes }
import org.hammerlab.bgzf.{ EstimatedCompressionRatio, Pos }
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.genomics.loci.parsing.ParsedLoci
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.{ Locus, Region }
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.{ FileSplits, MaxSplitSize }
import org.hammerlab.iterator.group.ElementTooCostlyStrategy.EmitAlone
import org.hammerlab.math.ceil
import org.seqdoop.hadoop_bam.{ BAMRecordReader, CRAMInputFormat, FileVirtualSplit, SAMRecordWritable }

import scala.collection.JavaConverters._
import scala.math.max

/**
 * Add a `loadBam` method to [[SparkContext]] for loading [[SAMRecord]]s from a BAM file.
 */
trait CanLoadBam
  extends Logging {

  import CanLoadBam._

  implicit def sc: SparkContext

  implicit def conf: Configuration = sc.hadoopConfiguration

  /**
   * Load [[SAMRecord]]s that overlap specific genomic intervals from an indexed BAM file.
   *
   * @param path Indexed BAM file path
   * @param intervals Genomic intervals to intersect with
   * @param splitSize divide the loaded file-portions into partitions of approximately this (uncompressed) size
   * @param estimatedCompressionRatio used for (approximately) sizing returned partitions: compressed sizes of BAI
   *                                  "chunks" are scaled by this value so that returned partitions are –
   *                                  theoretically/approximately – `splitSize` (uncompressed)
   * @return [[RDD]] of [[SAMRecord]]s that overlap the provided [[LociSet]]
   */
  def loadBamIntervals(path: Path,
                       splitSize: MaxSplitSize = MaxSplitSize(),
                       estimatedCompressionRatio: EstimatedCompressionRatio = default[EstimatedCompressionRatio]
  )(
      intervals: String*
  ): RDD[SAMRecord] =
    loadBamIntervals(
      path,
      LociSet(
        ParsedLoci(intervals.iterator),
        ContigLengths(path)
      ),
      splitSize,
      estimatedCompressionRatio
    )

  def loadBamIntervals(path: Path,
                       intervals: LociSet,
                       splitSize: MaxSplitSize,
                       estimatedCompressionRatio: EstimatedCompressionRatio): RDD[SAMRecord] = {

    val intervalsBroadcast = sc.broadcast(intervals)

    if (path.toString.endsWith(".sam")) {
      warn(s"Attempting to load SAM file $path with intervals filter")
      return loadSam(path, splitSize)
             .filter {
               record ⇒
                 region(record)
                   .exists(
                     intervalsBroadcast
                       .value
                       .intersects
                   )
             }
    }

    val header = Header(path)
    val headerBroadcast = sc.broadcast(header)

    val chunks = getIntevalChunks(path, intervals)

    val chunkPartitions =
      chunks
        .cappedCostGroups(
          _.size(estimatedCompressionRatio),
          splitSize.toDouble
        )
        .map(_.toVector)
        .toVector

    sc
      .parallelize(
        chunkPartitions,
        max(
          1,
          chunkPartitions.size
        )
      )
      .flatMap(chunks ⇒ chunks)
      .mapPartitions {
        chunks ⇒

          val compressedChannel = SeekableByteChannel(path).cache

          val uncompressedBytes = SeekableUncompressedBytes(compressedChannel)

          val records = SeekableRecordStream(uncompressedBytes, headerBroadcast.value)

          chunks
            .flatMap {
              chunk ⇒
                records.seek(chunk.start)

                new SimpleIterator[SAMRecord] {
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
              splitSize: MaxSplitSize = MaxSplitSize(),
              bgzfBlocksToCheck: BGZFBlocksToCheck = default[BGZFBlocksToCheck],
              readsToCheck: ReadsToCheck = default[ReadsToCheck],
              maxReadSize: MaxReadSize = default[MaxReadSize]
             ): RDD[SAMRecord] = {
    val headerBroadcast = sc.broadcast(Header(path))
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
                         bgzfBlocksToCheck: BGZFBlocksToCheck = default[BGZFBlocksToCheck],
                         readsToCheck: ReadsToCheck = default[ReadsToCheck],
                         maxReadSize: MaxReadSize = default[MaxReadSize]
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
                            splitSize: MaxSplitSize,
                            bgzfBlocksToCheck: BGZFBlocksToCheck,
                            readsToCheck: ReadsToCheck,
                            maxReadSize: MaxReadSize
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

          rs.takeWhile {
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
                bgzfBlocksToCheck: BGZFBlocksToCheck = default[BGZFBlocksToCheck],
                readsToCheck: ReadsToCheck = default[ReadsToCheck],
                maxReadSize: MaxReadSize = default[MaxReadSize],
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
