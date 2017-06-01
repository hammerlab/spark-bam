package org.hammerlab.bgzf.hadoop

import java.util

import org.apache.hadoop.conf.Configuration
import org.hammerlab.bgzf.block.Block
import org.hammerlab.bgzf.hadoop.RecordReader.make
import org.hammerlab.hadoop.{ FileInputFormat, FileSplit, FileSplits, Path }
import org.hammerlab.iterator.SimpleBufferedIterator

import scala.collection.JavaConverters._
import scala.io.Source.fromInputStream

case class IndexedInputFormat(override val path: Path,
                              indexPath: Path,
                              splits: Seq[Split],
                              blocksWhitelistOpt: Option[Set[Long]])
  extends FileInputFormat[Long, Block, Split, SimpleBufferedIterator[(Long, Block)]](path)

object IndexedInputFormat {

  def apply(path: Path,
            conf: Configuration,
            indexPath: Path = null,
            maxSplitSize: Int = -1,
            numBlocks: Int = -1,
            blocksWhitelist: Set[Long] = Set()): IndexedInputFormat =
    apply(
      path,
      conf,
      Option(indexPath).getOrElse(path.suffix(".blocks"): Path),
      if (maxSplitSize < 0) None else Some(maxSplitSize),
      if (numBlocks < 0) None else Some(numBlocks),
      if (blocksWhitelist.isEmpty)
        None
      else {
        Some(treeSet(blocksWhitelist))
      }
    )

  def apply(path: Path,
            conf: Configuration,
            numBlocksOpt: Option[Int],
            blocksWhitelistOpt: Option[Set[Long]]): IndexedInputFormat =
    apply(
      path,
      conf,
      path.suffix(".blocks"),
      maxSplitSizeOpt = None,
      numBlocksOpt,
      blocksWhitelistOpt.map(treeSet)
    )

  def apply(path: Path,
            conf: Configuration,
            indexPath: Path,
            maxSplitSizeOpt: Option[Int],
            numBlocksOpt: Option[Int],
            blocksWhitelistOpt: Option[util.NavigableSet[Long]]): IndexedInputFormat = {

    val fileSplits = FileSplits(path, conf, maxSplitSizeOpt)

    val fs = path.getFileSystem(conf)

    val indexStream = fs.open(indexPath)

    val (blocks, blocksWhitelist) = {

      val blockStarts =
        fromInputStream(indexStream)
        .getLines()
        .map(
          line ⇒
            line.split(",") match {
              case Array(block, _) ⇒
                block.toLong
              case _ ⇒
                throw new IllegalArgumentException(s"Bad blocks-index line: $line")
            }
        )

      (blocksWhitelistOpt, numBlocksOpt) match {
        case (Some(_), Some(_)) ⇒
          throw new IllegalArgumentException(
            s"Specify exactly one of {blocksWhitelist, numBlocks}"
          )
        case (Some(whitelist), _) ⇒
          blockStarts →
            Some(whitelist)
        case (_, Some(numBlocks)) ⇒
          val blocks = blockStarts.take(numBlocks).toList

          blocks.iterator →
            Some(treeSet(blocks))
        case _ ⇒
          blockStarts →
            None
      }
    }

    val blockPosMap = treeSet(blocks)

    val splits =
      fileSplits
        .flatMap {
          case FileSplit(_, start, length, locations) ⇒
            val end = start + length
            val blockStart = Option(blockPosMap.ceiling(start)).getOrElse(end)
            val blockEnd = Option(blockPosMap.ceiling(end)).getOrElse(end)

            val splitWhitelist: Option[Set[Long]] =
              blocksWhitelist
                .map(
                  _
                    .subSet(blockStart, blockEnd)
                    .asScala
                    .toSet
                )

            if (blockEnd > blockStart)
              Some(
                Split(
                  path,
                  blockStart,
                  blockEnd - blockStart,
                  locations,
                  splitWhitelist
                )
              )
            else
              None
        }

    new IndexedInputFormat(
      path,
      indexPath,
      splits,
      blocksWhitelist.map(_.asScala.toSet)
    )
  }

  def treeSet(blocks: Iterable[Long]): util.TreeSet[Long] = treeSet(blocks.iterator)
  def treeSet(blocks: Iterator[Long]): util.TreeSet[Long] = {
    val set = new util.TreeSet[Long]()
    for {
      block ← blocks
    } {
      set.add(block)
    }
    set
  }

}
