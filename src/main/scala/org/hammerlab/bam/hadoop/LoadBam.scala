package org.hammerlab.bam.hadoop

import htsjdk.samtools.SAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.Header
import org.hammerlab.bam.iterator.SeekableRecordStream
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ FindBlockStart, SeekableByteStream }
import org.hammerlab.hadoop.{ FileSplits, Path }
import org.hammerlab.io.ByteChannel.SeekableHadoopByteChannel
import org.hammerlab.io.CachingChannel
import org.hammerlab.iterator.SimpleBufferedIterator
import org.hammerlab.iterator.Sliding2Iterator._
import org.hammerlab.magic.rdd.hadoop.SerializableConfiguration._

/**
 * Add a `loadBam` method to [[SparkContext]] for loading [[SAMRecord]]s from a BAM file.
 */
object LoadBam {

  /**
   * Configuration options for BAM-loading.
   *
   * @param bgzfBlockHeadersToCheck when searching for bgzf-block-boundaries, check this many blocks ahead before
   *                                declaring a position to be a block-boundary
   * @param maxReadSize when searching for BAM-record-boundaries, try up to this many consecutive positions before
   *                    giving up / throwing; reads taking up more than this many bytes on disk can result in
   *                    "false-negative" read-boundary calls
   * @param maxSplitSize maximum split size to pass to [[org.apache.hadoop.mapreduce.lib.input.FileInputFormat]]
   */
  case class Config(bgzfBlockHeadersToCheck: Int = 5,
                    maxReadSize: Int = 1000000,
                    maxSplitSize: Option[Int] = None)

  implicit val default: Config = Config()

  implicit class LoadBamContext(val sc: SparkContext)
    extends AnyVal {

    def loadBam(path: Path)(implicit config: Config): RDD[SAMRecord] = {
      val conf = sc.hadoopConfiguration
      val fileSplitStarts = FileSplits(path, conf, config.maxSplitSize).map(_.start)

      val fileSplitStartsRDD =
        sc.parallelize(
          fileSplitStarts,
          fileSplitStarts.size
        )

      val confBroadcast = sc.broadcast(conf.serializable)

      val fs = path.getFileSystem(conf)
      val len = fs.getFileStatus(path).getLen
      val endPos = Pos(len, 0)

      val bamRecordStarts =
        fileSplitStartsRDD
            .map {
              fileSplitStart ⇒

                val compressedChannel: CachingChannel = SeekableHadoopByteChannel(path, confBroadcast.value)

                val uncompressedBytes = SeekableByteStream(compressedChannel)

                val header = Header(uncompressedBytes)
                val Header(contigLengths, _, _) = header

                val bgzfBlockStart =
                  FindBlockStart(
                    path,
                    fileSplitStart,
                    compressedChannel,
                    config.bgzfBlockHeadersToCheck
                  )

                val bamRecordStart =
                  FindRecordStart(
                    path,
                    uncompressedBytes,
                    bgzfBlockStart,
                    contigLengths,
                    config.maxReadSize
                  )

                uncompressedBytes.close()

                bamRecordStart
            }
            .collect
            .sliding2(endPos)
            .filter {
              case (start, end) ⇒
                end > start
            }
            .toVector

      val bamRecordStartsRDD = sc.parallelize(bamRecordStarts, bamRecordStarts.size)
      println(s"${bamRecordStarts.length} bam record starts:\n${bamRecordStarts.mkString("\n")}")

      bamRecordStartsRDD
        .flatMap {
          case (start, end) ⇒
            val uncompressedBytes =
              SeekableByteStream(
                SeekableHadoopByteChannel(
                  path,
                  confBroadcast.value
                )
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

              override protected def done(): Unit = {
                recordStream.close()
              }
            }
        }
    }
  }
}
