package org.hammerlab.bam.spark

import grizzled.slf4j.Logging
import htsjdk.samtools.SamReaderFactory.Option._
import htsjdk.samtools.{ BAMFileReader, QueryInterval, SAMLineParser, SAMRecord, SamReaderFactory }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.bam.iterator.SeekableRecordStream
import org.hammerlab.bgzf.block.{ FindBlockStart, SeekableUncompressedBytes }
import org.hammerlab.bgzf.{ EstimatedCompressionRatio, Pos }
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.{ Locus, Region }
import org.hammerlab.hadoop.{ Configuration, FileSplits, MaxSplitSize, Path }
import org.hammerlab.io.CachingChannel._
import org.hammerlab.io.SeekableByteChannel
import org.hammerlab.iterator.CappedCostGroupsIterator.ElementTooCostlyStrategy.EmitAlone
import org.hammerlab.iterator.CappedCostGroupsIterator._
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.iterator.SimpleBufferedIterator
import org.hammerlab.iterator.sliding.Sliding2Iterator._
import org.hammerlab.math.ceil
import org.hammerlab.parallel._
import org.hammerlab.parallel.spark.ElemsPerPartition
import org.hammerlab.{ bgzf, parallel, paths }
import org.seqdoop.hadoop_bam.util.SAMHeaderReader.readSAMHeaderFrom

import scala.collection.JavaConverters._

/**
 * Add a `loadBam` method to [[SparkContext]] for loading [[SAMRecord]]s from a BAM file.
 */
object LoadBam
  extends Logging {

  /**
   * Configuration options for BAM-loading.
   *
   * @param bgzfBlockHeadersToCheck when searching for bgzf-block-boundaries, check this many blocks ahead before
   *                                declaring a position to be a block-boundary
   * @param maxReadSize when searching for BAM-record-boundaries, try up to this many consecutive positions before
   *                    giving up / throwing; reads taking up more than this many bytes on disk can result in
   *                    "false-negative" read-boundary calls
   * @param maxSplitSize maximum split size to pass to [[org.apache.hadoop.mapreduce.lib.input.FileInputFormat]]
   * @param estimatedBamCompressionRatio used to estimate distances between [[Pos]]s / sizes of [[Chunk]]s
   */
  case class Config(bgzfBlockHeadersToCheck: Int = 5,
                    maxReadSize: Int = 1000000,
                    maxSplitSize: MaxSplitSize,
                    @transient parallelizer: parallel.Config = implicitly[threads.Config],
                    estimatedBamCompressionRatio: EstimatedCompressionRatio = 3.0)
    extends bgzf.hadoop.Config

  object Config {
    implicit def makeConfigFromParallelizerWithContext(parallelizer: parallel.Config)(
        implicit sc: SparkContext
    ): Config =
      Config(
        parallelizer = parallelizer,
        maxSplitSize = MaxSplitSize()(sc)
      )

    implicit def makeConfigFromParallelizerWithConfiguration(parallelizer: parallel.Config)(
        implicit conf: Configuration
    ): Config =
      Config(
        parallelizer = parallelizer,
        maxSplitSize = MaxSplitSize()
      )

    implicit def makeConfigFromParallelizerWithMaxSplitSize(parallelizer: parallel.Config)(
        implicit maxSplitSize: MaxSplitSize
    ): Config =
      Config(
        parallelizer = parallelizer,
        maxSplitSize = maxSplitSize
      )

    implicit def defaultConfig(implicit
                               sc: SparkContext,
                               parallelizer: parallel.Config = implicitly[threads.Config]
                              ): Config =
      Config(
        maxSplitSize = MaxSplitSize()(sc),
        parallelizer = parallelizer
      )
  }

  // When computing splits using a Spark job, default to one split per task
  implicit val defaultPartitioningStrategy = ElemsPerPartition(1)

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

    val samReader =
      readerFactory
        .open(
          paths.Path(
            path.filesystem.makeQualified(path).toUri
          )
        )

    val header = readSAMHeaderFrom(path, conf)
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

    val span = BAMFileReader.getFileSpan(queryIntervals, idx)

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


  trait CanLoadBam
    extends Any {

    def sc: SparkContext

    implicit def conf: Configuration = sc.hadoopConfiguration

    def loadBamIntervals(path: Path,
                         intervals: LociSet)(
        implicit config: Config
    ): RDD[SAMRecord] = {

      val intervalsBroadcast = sc.broadcast(intervals)

      if (path.toString.endsWith(".sam")) {
        warn(s"Attempting to load SAM file $path with intervals filter")
        return loadSam(path)
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

      implicit val estimatedBamCompressionRatio = config.estimatedBamCompressionRatio

      val chunkPartitions =
        chunks
          .iterator
          .cappedCostGroups(
            _.size,
            config
              .maxSplitSize
              .toDouble
          )
          .map(_.toVector)
          .toVector

      val confBroadcast = sc.broadcast(conf)

      sc
        .parallelize(
          chunkPartitions,
          chunkPartitions.size
        )
        .flatMap(chunks ⇒ chunks)
        .mapPartitions {
          chunks ⇒

            val compressedChannel = SeekableByteChannel(path)(confBroadcast).cache

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

    def loadSam(path: Path)(implicit config: Config): RDD[SAMRecord] = {
      val header = sc.broadcast(readSAMHeaderFrom(path, conf))
      sc
        .textFile(
          path.toString,
          minPartitions =
            ceil[Long](
              path.length,
              config.maxSplitSize
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

    def loadBam(path: Path)(implicit config: Config): BAMRecordRDD = {

      val fileSplitStarts = FileSplits(path, conf).map(_.start)

      val confBroadcast = sc.broadcast(conf)

      val fs = path.getFileSystem(conf)
      val len = fs.getFileStatus(path).getLen
      val endPos = Pos(len, 0)

      val contigLengthsBroadcast = sc.broadcast(ContigLengths(path))

      implicit val parallelizer = config.parallelizer

      val splits =
        fileSplitStarts
            .pmap {
              fileSplitStart ⇒

                val compressedChannel =
                  SeekableByteChannel(path)(confBroadcast).cache

                val bgzfBlockStart =
                  FindBlockStart(
                    path,
                    fileSplitStart,
                    compressedChannel,
                    config.bgzfBlockHeadersToCheck
                  )

                val uncompressedBytes = SeekableUncompressedBytes(compressedChannel)

                val bamRecordStart =
                  FindRecordStart(
                    path,
                    uncompressedBytes,
                    bgzfBlockStart,
                    contigLengthsBroadcast.value,
                    config.maxReadSize
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
                  SeekableByteChannel(path)(confBroadcast).cache
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

    def loadReads(path: Path)(implicit config: Config): RDD[SAMRecord] =
      if (path.toString.endsWith(".sam")) {
        loadSam(path)
      } else if (path.toString.endsWith(".bam"))
        loadBam(path)
      else
        throw new IllegalArgumentException(s"Can't load reads from path: $path")
  }
}
