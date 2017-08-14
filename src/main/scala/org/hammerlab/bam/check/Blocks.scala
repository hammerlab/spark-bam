package org.hammerlab.bam.check

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.bgzf.block.{ FindBlockStart, Metadata, MetadataStream }
import org.hammerlab.bytes._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.magic.rdd.partitions.PartitionByKeyRDD._
import org.hammerlab.math.MonoidSyntax._
import org.hammerlab.math.Monoid.zero
import org.hammerlab.math.ceil
import org.hammerlab.paths.Path
import org.hammerlab.spark.Context

case class Whitelist(blocks: Set[Long])

object Whitelist {

  trait Args {
    def blocksWhitelist: Option[String]
  }

  def apply(blocks: Option[Set[Long]]): Option[Whitelist] =
    blocks.map(apply)

  def apply(args: Args): Option[Whitelist] =
    apply(
      args
        .blocksWhitelist
        .map(
          _
            .split(",")
            .map(_.toLong)
            .toSet
        )
    )
}

case class Blocks(partitionedBlocks: RDD[Metadata],
                  uncompressedSize: Long,
                  numBlocks: Long,
                  whitelist: Option[Whitelist],
                  whitelistBroadcast: Broadcast[Option[Whitelist]])

object Blocks {

  trait Args
    extends Whitelist.Args {
    def blocks: Option[Path]
    def splitSize: Option[Bytes]
    def bgzfBlockHeadersToCheck: Int
    def numBlocks: Option[Int]
    def blocksPerPartition: Int
  }

  def apply(args: Args)(
      implicit
      sc: Context,
      path: Path
  ): (RDD[Metadata], Option[Whitelist]) = {

    val blocksPath: Path =
      args
        .blocks
        .getOrElse(
          path + ".blocks"
        )

    /** Parse BGZF-block [[Metadata]] emitted by [[org.hammerlab.bgzf.index.IndexBlocks]] */
    val allBlocks =
      if (blocksPath.exists)
        sc
        .textFile(blocksPath.toString)
        .map(
          line ⇒
            line.split(",") match {
              case Array(block, compressedSize, uncompressedSize) ⇒
                Metadata(
                  block.toLong,
                  compressedSize.toInt,
                  uncompressedSize.toInt
                )
              case _ ⇒
                throw new IllegalArgumentException(
                  s"Bad blocks-index line: $line"
                )
            }
        )
      else {
        val splitSize = args.splitSize.getOrElse(1.MB).bytes
        val numSplits = ceil(path.size, splitSize).toInt
        sc
          .parallelize(
            0 until numSplits,
            numSplits
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
              MetadataStream(in).takeWhile(_.start < end)
          }
      }

    /**
     * Apply any applicable filters to [[allBlocks]]; also store the set of filtered blocks, if applicable.
     */
    val (blocks, whitelist) =
      (Whitelist(args), args.numBlocks) match {
        case (Some(_), Some(_)) ⇒
          throw new IllegalArgumentException(
            s"Specify exactly one of {blocksWhitelist, numBlocks}"
          )
        case (Some(whitelist), _) ⇒
          (
            allBlocks
              .filter {
                case Metadata(block, _, _) ⇒
                  whitelist.blocks(block)
              },
              Some(whitelist)
          )
        case (_, Some(numBlocks)) ⇒
          val filteredBlocks = allBlocks.take(numBlocks)
          (
            sc.parallelize(
              filteredBlocks,
              ceil(
                numBlocks,
                args.blocksPerPartition
              )
            ),
            Some(
              Whitelist(
                filteredBlocks
                  .map(_.start)
                  .toSet
              )
            )
          )
        case _ ⇒
          (allBlocks, None)
      }

    (blocks, whitelist)
  }

  def register(implicit kryo: Kryo): Unit = {
    kryo.register(classOf[Whitelist])
    kryo.register(classOf[Range])
  }
}
