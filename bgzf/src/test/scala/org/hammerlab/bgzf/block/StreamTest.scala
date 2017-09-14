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
    blocks.size should be(30)

    blocks(0) should ===(Metadata(    0, 26169, 65498))
    blocks(1) should ===(Metadata(26169, 24080, 65498))
    blocks(2) should ===(Metadata(50249, 25542, 65498))

    val compressedStats =
      Stats(
        blocks.map(_.compressedSize)
      )

    check(
      compressedStats,
      """N: 30, μ/σ: 21141.4/3411.4, med/mad: 20825.5/1499
        | elems: 26169 24080 25542 22308 20688 19943 20818 21957 19888 20517 … 17812 19769 20223 20833 22341 23036 22645 24380 26254 7821
        |sorted: 7821 17812 18340 18636 18685 19368 19673 19769 19888 19943 … 22341 22645 22709 23036 24080 24380 25542 26169 26240 26254
        |   5:	13316.1
        |  10:	18369.6
        |  25:	19745
        |  50:	20825.5
        |  75:	22790.8
        |  90:	26106.3
        |  95:	26246.3"""
    )

    val prunedCompressedStats =
      Stats(
        blocks
          .map(_.compressedSize)
          .drop(1)
          .dropRight(1)
      )

    check(
      prunedCompressedStats,
      """N: 28, μ/σ: 21437.6/2267.5, med/mad: 20825.5/1470
        | elems: 24080 25542 22308 20688 19943 20818 21957 19888 20517 21636 … 18685 17812 19769 20223 20833 22341 23036 22645 24380 26254
        |sorted: 17812 18340 18636 18685 19368 19673 19769 19888 19943 20223 … 22308 22341 22645 22709 23036 24080 24380 25542 26240 26254
        |   5:	18049.6
        |  10:	18606.4
        |  25:	19798.8
        |  50:	20825.5
        |  75:	22693
        |  90:	25611.8
        |  95:	26247.7"""
    )

    val uncompressedStats =
      Stats(
        blocks.map(_.uncompressedSize)
      )

    check(
      uncompressedStats,
      """N: 30, μ/σ: 64053.8/7777.3, med/mad: 65498/0
        | elems: 65498×29 22172
        |sorted: 22172 65498×29
        |   5:	46001.3
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
          .drop(1)
          .dropRight(1)
      )

    check(
      prunedUncompressedStats,
      """N: 28, μ/σ: 65498/0, med/mad: 65498/0
        | elems: 65498×28
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
