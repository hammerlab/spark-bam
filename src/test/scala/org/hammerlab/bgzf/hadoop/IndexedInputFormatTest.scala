package org.hammerlab.bgzf.hadoop

import org.hammerlab.bgzf.block.Block
import org.hammerlab.hadoop.{ FileSplits, Path }
import org.hammerlab.iterator.SimpleBufferedIterator
import org.hammerlab.magic.rdd.hadoop.HadoopRDD._
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File
import RecordReader.make

class IndexedInputFormatTest
  extends SparkSuite {

  lazy val hc = sc.hadoopConfiguration

  def check(maxSplitSize: Int, expected: ExpectedSplit*): Unit =
    check(maxSplitSize, None, expected)

  def check(maxSplitSize: Int, numBlocks: Int, expected: ExpectedSplit*): Unit =
    check(maxSplitSize, Some(numBlocks), expected)

  case class ExpectedSplit(start: Long, end: Long, whitelist: Option[Set[Long]])

  implicit def convTuple2(t: (Int, Int)): ExpectedSplit =
    ExpectedSplit(
      t._1,
      t._2,
      None
    )

  implicit def convTuple3(t: ((Int, Int), Seq[Int])): ExpectedSplit =
    ExpectedSplit(
      t._1._1,
      t._1._2,
      Some(t._2.map(_.toLong).toSet)
    )

  def check(maxSplitSize: Int,
            numBlocks: Option[Int],
            expected: Seq[ExpectedSplit]): Unit = {

    val path = Path(File("5k.bam").uri)
    val fmt =
      IndexedInputFormat(
        path,
        hc,
        maxSplitSize = maxSplitSize,
        numBlocks = numBlocks.getOrElse(-1)
      )

    val hostname = FileSplits(path, hc).head.locations.head

    fmt.splits should be(
      expected
        .map {
          case ExpectedSplit(start, end, whitelist) ⇒
            Split(
              path,
              start,
              end - start,
              Array(hostname),
              whitelist
            )
        }
    )
  }

  test("5k-max") {
    check(
      1500000,
      0 → 1010703
    )
  }

  test("5k-900k") {
    check(
      900000,
           0 →  905238,
      905238 → 1010703
    )
  }

  test("5k-100k") {
    check(
      100000,
            0 →  118432,
       118432 →  219987,
       219987 →  310975,
       310975 →  406097,
       406097 →  501675,
       501675 →  618149,
       618149 →  718074,
       718074 →  800863,
       800863 →  905238,
       905238 → 1006167,
      1006167 → 1010703
    )
  }

  test("5k-100k-12") {
    check(
      100000,
      numBlocks = 12,
           0 →  118432 → Seq(     0,   2454,  27784,  51386,  76438, 98118),
      118432 →  219987 → Seq(118432, 138207, 158603, 180136, 199780),
      219987 →  300000 → Seq(219987)
    )
  }

  test("load blocks RDD") {
    val path = new Path(File("5k.bam").uri)
    val fmt =
      IndexedInputFormat(
        path,
        hc,
        maxSplitSize = 100000
      )

    val blocks =
      sc.loadHadoopRDD[Long, Block, Split, SimpleBufferedIterator[(Long, Block)]](
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
        maxSplitSize = 100000,
        numBlocks = 8
      )

    val blocks =
      sc.loadHadoopRDD[Long, Block, Split, SimpleBufferedIterator[(Long, Block)]](
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
        maxSplitSize = 100000,
        blocksWhitelist = Set[Long](180136, 284685, 310975, 501675,778353)
      )

    val blocks =
      sc.loadHadoopRDD[Long, Block, Split, SimpleBufferedIterator[(Long, Block)]](
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
