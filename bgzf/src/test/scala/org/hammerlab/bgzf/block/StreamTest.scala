package org.hammerlab.bgzf.block

import java.nio.channels.FileChannel

import hammerlab.indent.implicits.tab
import hammerlab.lines._
import hammerlab.math.sigfigs._
import hammerlab.show._
import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.stats.Stats
import org.hammerlab.test.Suite

class StreamTest
  extends Suite {

  implicit val sigfigs: SigFigs = 3

  def check(stats: Stats[Int, Int], expectedStr: String): Unit = {
    stats.showLines should be(
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
      """N: 25, μ/σ: 21269/3356, med/mad: 20818/1620
        | elems: 26169 24080 25542 22308 20688 19943 20818 21957 19888 20517 … 22438 20691 19815 18922 20693 26727 19157 18200 17815 9929
        |sorted: 9929 17815 18200 18922 19157 19815 19888 19943 20517 20688 … 21957 22308 22438 22709 23310 24080 25542 26169 26240 26727
        |  .05:	12295
        |  .10:	18046
        |  .25:	19852
        |  .50:	20818
        |  .75:	23010
        |  .90:	26197
        |  .95:	26581"""
    )

    val prunedCompressedStats =
      Stats(
        blocks
          .map(_.compressedSize)
          .dropRight(1)
      )

    check(
      prunedCompressedStats,
      """N: 24, μ/σ: 21742/2479, med/mad: 21176/1324
        | elems: 26169 24080 25542 22308 20688 19943 20818 21957 19888 20517 … 23310 22438 20691 19815 18922 20693 26727 19157 18200 17815
        |sorted: 17815 18200 18922 19157 19815 19888 19943 20517 20688 20691 … 21957 22308 22438 22709 23310 24080 25542 26169 26240 26727
        |  .05:	17911
        |  .10:	18561
        |  .25:	19902
        |  .50:	21176
        |  .75:	23160
        |  .90:	26205
        |  .95:	26605"""
    )

    val uncompressedStats =
      Stats(
        blocks.map(_.uncompressedSize)
      )

    check(
      uncompressedStats,
      """N: 25, μ/σ: 64261/6061, med/mad: 65498/0
        | elems: 65498×24 34570
        |sorted: 34570 65498×24
        |  .05:	43848
        |  .10:	65498"""
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
        |  .95:	65498"""
    )
  }

}
