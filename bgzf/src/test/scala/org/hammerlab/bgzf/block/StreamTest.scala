package org.hammerlab.bgzf.block

import java.io.FileInputStream
import java.nio.channels.FileChannel

import cats.implicits.catsStdShowForInt
import cats.syntax.all._
import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.stats.Stats
import org.hammerlab.test.Suite

class StreamTest
  extends Suite {

  def check(stats: Stats[Int, Int], expectedStr: String): Unit = {
    stats.show should be(
      expectedStr
        .stripMargin
        .trim
    )
  }

  implicit def blockToMetadata(block: Block): Metadata =
    Metadata(
      block.start,
      block.compressedSize,
      block.uncompressedSize
    )

  test("seekable") {

    val ch = FileChannel.open(bam2.path)
    val stream = SeekableStream(ch)

    stream.next() should ===(Metadata(    0, 26169, 65498))
    stream.seek(0)
    stream.next() should ===(Metadata(    0, 26169, 65498))
    stream.seek(0)
    stream.next() should ===(Metadata(    0, 26169, 65498))
    stream.next() should ===(Metadata(26169, 24080, 65498))

    stream.seek(0)
    stream.next() should ===(Metadata(    0, 26169, 65498))

    stream.seek(75791)
    stream.next() should ===(Metadata(75791, 22308, 65498))
  }

  test("2.bam") {
    val is = bam2.inputStream
    val bgzfStream = Stream(is)
    val blocks = bgzfStream.toList
    blocks.size should be(25)

    blocks(0) should ===(Metadata(    0, 26169, 65498))
    blocks(1) should ===(Metadata(26169, 24080, 65498))
    blocks(2) should ===(Metadata(50249, 25542, 65498))

    val compressedStats =
      Stats(
        blocks.map(_.compressedSize)
      )

    check(
      compressedStats,
      """N: 25, μ/σ: 21269/3355.6, med/mad: 20818/1620
        | elems: 26169 24080 25542 22308 20688 19943 20818 21957 19888 20517 … 22438 20691 19815 18922 20693 26727 19157 18200 17815 9929
        |sorted: 9929 17815 18200 18922 19157 19815 19888 19943 20517 20688 … 21957 22308 22438 22709 23310 24080 25542 26169 26240 26727
        |   5:	12294.8
        |  10:	18046
        |  25:	19851.5
        |  50:	20818
        |  75:	23009.5
        |  90:	26197.4
        |  95:	26580.9"""
    )

    val prunedCompressedStats =
      Stats(
        blocks
          .map(_.compressedSize)
          .dropRight(1)
      )

    check(
      prunedCompressedStats,
      """N: 24, μ/σ: 21741.5/2479.5, med/mad: 21175.5/1324
        | elems: 26169 24080 25542 22308 20688 19943 20818 21957 19888 20517 … 23310 22438 20691 19815 18922 20693 26727 19157 18200 17815
        |sorted: 17815 18200 18922 19157 19815 19888 19943 20517 20688 20691 … 21957 22308 22438 22709 23310 24080 25542 26169 26240 26727
        |   5:	17911.3
        |  10:	18561
        |  25:	19901.8
        |  50:	21175.5
        |  75:	23159.8
        |  90:	26204.5
        |  95:	26605.3"""
    )

    val uncompressedStats =
      Stats(
        blocks.map(_.uncompressedSize)
      )

    check(
      uncompressedStats,
      """N: 25, μ/σ: 64260.9/6060.6, med/mad: 65498/0
        | elems: 65498×24 34570
        |sorted: 34570 65498×24
        |   5:	43848.4
        |  10:	65498
        |  25:	65498
        |  50:	65498
        |  75:	65498
        |  90:	65498
        |  95:	65498"""
    )

    val prunedUncompressedStats =
      Stats(
        blocks
          .map(_.uncompressedSize)
          .dropRight(1)
      )

    check(
      prunedUncompressedStats,
      """N: 24, μ/σ: 65498/0, med/mad: 65498/0
        | elems: 65498×24
        |   5:	65498
        |  10:	65498
        |  25:	65498
        |  50:	65498
        |  75:	65498
        |  90:	65498
        |  95:	65498"""
    )
  }

}
