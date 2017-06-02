package org.hammerlab.bgzf.hadoop

import java.net.URI
import java.util

import org.apache.hadoop.conf.Configuration
import org.hammerlab.bgzf.block.Metadata
import org.hammerlab.hadoop.{ FileSplits, Path }

import scala.io.Source.fromInputStream

case class IndexedInputFormat(path: Path,
                              indexPath: Path,
                              splits: Seq[BlocksSplit])

object IndexedInputFormat {

  def apply(path: Path,
            conf: Configuration,
            indexPath: Path = null,
            blocksPerPartition: Int,
            numBlocks: Int = -1,
            blocksWhitelist: Set[Long] = Set()): IndexedInputFormat =
    apply(
      path,
      conf,
      Option(indexPath).getOrElse(path.suffix(".blocks"): Path),
      blocksPerPartition,
      if (numBlocks < 0) None else Some(numBlocks),
      if (blocksWhitelist.isEmpty)
        None
      else {
        Some(treeSet(blocksWhitelist))
      }
    )

  def apply(path: Path,
            conf: Configuration,
            indexPathOpt: Option[String],
            blocksPerPartition: Int,
            numBlocksOpt: Option[Int],
            blocksWhitelistOpt: Option[Set[Long]]): IndexedInputFormat =
    apply(
      path,
      conf,
      indexPathOpt
        .map(str ⇒ Path(new URI(str)))
        .getOrElse(path.suffix(".blocks"): Path),
      blocksPerPartition,
      numBlocksOpt,
      blocksWhitelistOpt.map(treeSet)
    )

  def apply(path: Path,
            conf: Configuration,
            indexPath: Path,
            blocksPerPartition: Int,
            numBlocksOpt: Option[Int],
            blocksWhitelistOpt: Option[util.NavigableSet[Long]]): IndexedInputFormat = {

    val fileSplits = FileSplits(path, conf)

    val fs = path.getFileSystem(conf)

    val indexStream = fs.open(indexPath)

    val allBlocks =
      fromInputStream(indexStream)
        .getLines()
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
                throw new IllegalArgumentException(s"Bad blocks-index line: $line")
            }
        )

    val blocks =
      (blocksWhitelistOpt, numBlocksOpt) match {
        case (Some(_), Some(_)) ⇒
          throw new IllegalArgumentException(
            s"Specify exactly one of {blocksWhitelist, numBlocks}"
          )
        case (Some(whitelist), _) ⇒
          allBlocks
            .filter {
              case Metadata(block, _, _) ⇒
                whitelist.contains(block)
            }
        case (_, Some(numBlocks)) ⇒
          allBlocks.take(numBlocks)
        case _ ⇒
          allBlocks
      }

    val fileSplitsMap = treeSet(fileSplits.map(_.start))

    val fileSplitsByStart =
      fileSplits
        .map(fs ⇒ fs.start → fs)
        .toMap

    val splits =
      blocks
        .grouped(blocksPerPartition)
        .map {
          blocks ⇒
            val Metadata(start, _, _) = blocks.head
            val fileSplitStart = fileSplitsMap.floor(start)
            val fileSplit = fileSplitsByStart(fileSplitStart)
            BlocksSplit(
              path,
              blocks,
              fileSplit.locations
            )
        }
        .toList

    new IndexedInputFormat(
      path,
      indexPath,
      splits
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
