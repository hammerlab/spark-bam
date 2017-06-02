package org.hammerlab.bgzf.hadoop

import org.hammerlab.bgzf.block.Metadata
import org.hammerlab.bgzf.hadoop.RecordReader.MetadataReader
import org.hammerlab.hadoop.{ FileSplits, Path }
import org.hammerlab.magic.rdd.hadoop.HadoopRDD._
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File

class IndexedInputFormatTest
  extends SparkSuite {

  lazy val hc = sc.hadoopConfiguration

  def check(blocksPerPartition: Int, expected: ExpectedSplit*): Unit =
    check(blocksPerPartition, None, expected)

  def check(blocksPerPartition: Int, numBlocks: Int, expected: ExpectedSplit*): Unit =
    check(blocksPerPartition, Some(numBlocks), expected)

  case class ExpectedSplit(start: Long, end: Long, numBlocks: Int)

  implicit def convTuple3(t: ((Int, Int), Int)): ExpectedSplit =
    ExpectedSplit(
      t._1._1,
      t._1._2,
      t._2
    )

  def check(blocksPerPartition: Int,
            numBlocks: Option[Int],
            expected: Seq[ExpectedSplit]): Unit = {

    val path = Path(File("5k.bam").uri)
    val fmt =
      IndexedInputFormat(
        path,
        hc,
        blocksPerPartition = blocksPerPartition,
        numBlocks = numBlocks.getOrElse(-1)
      )

    val hostname = FileSplits(path, hc).head.locations.head

    for {
      ((actual, expected), idx) ← fmt.splits.zip(expected).zipWithIndex
    } {
      withClue(s"Split $idx: $actual vs. $expected\n") {
        actual.path should be(path)
        actual.start should be(expected.start)
        actual.end should be(expected.end)
        actual.blocks.length should be(expected.numBlocks)
        actual.locations should be(Array(hostname))
      }
    }
  }

  test("5k.bam: 1 block per partition") {
    check(
      blocksPerPartition = 60,  // 1 split: 5k.bam is 50 blocks
      0 → 1010675 → 50
    )
  }

  test("5k.bam: 2 blocks per partition") {
    check(
      blocksPerPartition = 40,
           0 →  825923 → 40,
      825923 → 1010675 → 10
    )
  }

  test("5k.bam: 5 blocks per partition") {
    check(
      5,
            0 →   98118 → 5,
        98118 →  199780 → 5,
       199780 →  310975 → 5,
       310975 →  406097 → 5,
       406097 →  501675 → 5,
       501675 →  618149 → 5,
       618149 →  718074 → 5,
       718074 →  825923 → 5,
       825923 →  928569 → 5,
       928569 → 1010675 → 5
    )
  }

  test("5k.bam: 5-block splits, 12 blocks") {
    check(
      blocksPerPartition = 5,
      numBlocks = 12,
           0 →   98118 → 5,
       98118 →  199780 → 5,
      199780 →  241208 → 2
    )
  }

  test("load blocks RDD") {
    val path = new Path(File("5k.bam").uri)
    val fmt =
      IndexedInputFormat(
        path,
        hc,
        blocksPerPartition = 5
      )

    val blocks =
      sc.loadHadoopRDD[Long, Metadata, BlocksSplit](
        path,
        fmt.splits
      )
      .values
      .collect

    blocks.length should be(50)
    blocks
      .take(10)
      .map(
        block ⇒
          block.start →
            (block.compressedSize, block.uncompressedSize)
      ) should be(
      Array(
             0 → ( 2454,  5650),
          2454 → (25330, 65092),
         27784 → (23602, 64902),
         51386 → (25052, 65248),
         76438 → (21680, 64839),
         98118 → (20314, 64643),
        118432 → (19775, 65187),
        138207 → (20396, 64752),
        158603 → (21533, 64893),
        180136 → (19644, 64960)
      )
    )
  }

  test("load truncated RDD") {
    val path = new Path(File("5k.bam").uri)

    val fmt =
      IndexedInputFormat(
        path,
        hc,
        blocksPerPartition = 2,
        numBlocks = 8
      )

    val blocks =
      sc.loadHadoopRDD[Long, Metadata, BlocksSplit](
        path,
        fmt.splits
      )
      .values
      .collect

    blocks.length should be(8)
    blocks
      .map(
        block ⇒
          block.start →
            (block.compressedSize, block.uncompressedSize)
      ) should be(
      Array(
             0 → ( 2454,  5650),
          2454 → (25330, 65092),
         27784 → (23602, 64902),
         51386 → (25052, 65248),
         76438 → (21680, 64839),
         98118 → (20314, 64643),
        118432 → (19775, 65187),
        138207 → (20396, 64752)
      )
    )
  }

  test("blocks whitelist") {
    val path = new Path(File("5k.bam").uri)
    val fmt =
      IndexedInputFormat(
        path,
        hc,
        blocksPerPartition = 2,
        blocksWhitelist = Set[Long](180136, 284685, 310975, 501675,778353)
      )

    val blocks =
      sc.loadHadoopRDD[Long, Metadata, BlocksSplit](
        path,
        fmt.splits
      )
      .values
      .collect

    blocks.length should be(5)
    blocks
    .map(
      block ⇒
        block.start →
          (block.compressedSize, block.uncompressedSize)
    ) should be(
      Array(
        180136 → (19644, 64960),
        284685 → (26290, 65171),
        310975 → (20229, 65142),
        501675 → (21993, 64758),
        778353 → (22510, 65170)
      )
    )
  }
}
