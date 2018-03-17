package org.hammerlab.bam.spark.load

import hammerlab.iterator._
import hammerlab.path._
import htsjdk.samtools.BAMFileReader.getFileSpan
import htsjdk.samtools.SamReaderFactory.Option.{ CACHE_FILE_BASED_INDEXES, EAGERLY_DECODE }
import htsjdk.samtools.{ QueryInterval, SAMRecord, SamReaderFactory }
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.header.ContigLengths.readSAMHeaderFromStream
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.bam.iterator.SeekableRecordStream
import org.hammerlab.bam.spark.load.Intervals.{ getIntevalChunks, region }
import org.hammerlab.bgzf.EstimatedCompressionRatio
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.genomics.loci.parsing.ParsedLoci
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.{ Locus, Region }
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.iterator.group.ElementTooCostlyStrategy.EmitAlone
import shapeless.the

import scala.collection.JavaConverters._
import scala.math.max

trait Intervals {

  self: CanLoadBam ⇒

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
  def loadBamIntervals(
    path: Path,
    splitSize: MaxSplitSize = MaxSplitSize(),
    estimatedCompressionRatio: EstimatedCompressionRatio = the[EstimatedCompressionRatio]
  )(
    intervals: String*
  ): RDD[SAMRecord] =
    loadBamIntervals(
      path,
      LociSet(
        ParsedLoci(intervals.iterator),
        ContigLengths(path)
      )
    )(
      splitSize,
      estimatedCompressionRatio
    )

  def loadBamIntervals(
    path: Path,
    intervals: String*
  )(
    implicit
    splitSize: MaxSplitSize,
    ratio: EstimatedCompressionRatio
  ): RDD[SAMRecord] =
    loadBamIntervals(
      path,
      LociSet(
        ParsedLoci(intervals.iterator),
        ContigLengths(path)
      )
    )

  def loadBamIntervals(
    path: Path,
    intervals: LociSet
  )(
    implicit
    splitSize: MaxSplitSize,
    r: EstimatedCompressionRatio
  ): RDD[SAMRecord] = {

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

    // TODO: cram

    val header = Header(path)
    val headerBroadcast = sc.broadcast(header)

    val chunks = getIntevalChunks(path, intervals)

    val chunkPartitions =
      chunks
        .cappedCostGroups(
          _.size,
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


}

object Intervals {

  def getIntevalChunks(
    path: Path,
    intervals: LociSet
  )(
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
