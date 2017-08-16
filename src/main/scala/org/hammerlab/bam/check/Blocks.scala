package org.hammerlab.bam.check

import java.lang.{ Long ⇒ JLong }
import cats.implicits.catsKernelStdGroupForLong
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.rdd.RDD
import org.hammerlab.bgzf.block.{ FindBlockStart, Metadata, MetadataStream }
import org.hammerlab.bytes._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.guava.collect.Range.closedOpen
import org.hammerlab.guava.collect.RangeSet
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.magic.rdd.partitions.PartitionByKeyRDD._
import org.hammerlab.magic.rdd.scan.ScanLeftValuesRDD._
import org.hammerlab.magic.rdd.scan.ScanValuesRDD
import org.hammerlab.math.ceil
import org.hammerlab.paths.Path
import org.hammerlab.spark.Context

object Blocks {

  trait Args {
    def blocks: Option[Path]
    def splitSize: Option[Bytes]
    def bgzfBlockHeadersToCheck: Int
    @transient def ranges: Option[RangeSet[JLong]]
  }

  def apply(args: Args)(
      implicit
      sc: Context,
      path: Path
  ): RDD[Metadata] = {

    val blocksPath: Path =
      args
        .blocks
        .getOrElse(
          path + ".blocks"
        )

    val splitSize = args.splitSize.getOrElse(2.MB).bytes

    val rangeSetBroadcast = sc.broadcast(args.ranges)

    /** Parse BGZF-block [[Metadata]] emitted by [[org.hammerlab.bgzf.index.IndexBlocks]] */
    val blocks =
      if (blocksPath.exists) {
        val blocks =
          sc
            .textFile(blocksPath.toString)
            .map(
              line ⇒
                line.split(",") match {
                  case Array(start, compressedSize, uncompressedSize) ⇒
                    Metadata(
                      start.toLong,
                      compressedSize.toInt,
                      uncompressedSize.toInt
                    )
                  case _ ⇒
                    throw new IllegalArgumentException(
                      s"Bad blocks-index line: $line"
                    )
                }
            )
            .filter {
              case Metadata(start, _, _) ⇒
                rangeSetBroadcast
                  .value
                  .forall(
                    _.contains(start)
                  )
            }

        val ScanValuesRDD(scanRDD, _, total) =
          blocks
            .map {
              block ⇒
                block →
                  block
                    .compressedSize
                    .toLong
            }
            .scanLeftValues

        val numPartitions =
          ceil(
            total,
            splitSize
          )
          .toInt

        scanRDD
          .map {
            case (block, offset) ⇒
              (offset / splitSize).toInt →
                block.start →
                block
          }
          .partitionByKey(numPartitions)
      } else {
        val numSplits = ceil(path.size, splitSize).toInt
        val splitIdxs =
          0 until numSplits filter {
            idx ⇒
              val start = idx * splitSize
              val end = (idx + 1) * splitSize
              val range = closedOpen[JLong](start, end)
              rangeSetBroadcast
                .value
                .forall(
                  !_
                    .subRangeSet(range)
                    .isEmpty
                )
          }
        sc
          .parallelize(
            splitIdxs,
            splitIdxs.length
          )
          .flatMap {
            idx ⇒
              val start = idx * splitSize
              val end = (idx + 1) * splitSize
              val in = SeekableByteChannel(path)
              val blockStart =
                FindBlockStart(
                  path,
                  start,
                  in,
                  args.bgzfBlockHeadersToCheck
                )

              in.seek(blockStart)

              MetadataStream(in)
                .takeWhile(_.start < end)
                .filter {
                  case Metadata(start, _, _) ⇒
                    rangeSetBroadcast
                      .value
                      .forall(
                        _.contains(start)
                      )
                }
                .finish(in.close())
          }
      }

    blocks
  }

  def register(implicit kryo: Kryo): Unit = {
    kryo.register(classOf[Range])
  }
}
