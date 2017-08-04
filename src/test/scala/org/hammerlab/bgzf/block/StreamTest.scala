package org.hammerlab.bgzf.block

import java.io.FileInputStream
import java.nio.channels.FileChannel

import cats.implicits.catsStdShowForInt
import cats.syntax.all._
import org.hammerlab.resources.bam5k
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

    val ch = FileChannel.open(bam5k.path)
    val stream = SeekableStream(ch)

    stream.next() should ===(Metadata(    0,  2454,  5650))
    stream.seek(0)
    stream.next() should ===(Metadata(    0,  2454,  5650))
    stream.seek(0)
    stream.next() should ===(Metadata(    0,  2454,  5650))
    stream.next() should ===(Metadata( 2454, 25330, 65092))

    stream.seek(0)
    stream.next() should ===(Metadata(    0,  2454,  5650))

    stream.seek(27784)
    stream.next() should ===(Metadata(27784, 23602, 64902))
  }

  test("5k.bam") {
    val is = new FileInputStream(bam5k)
    val bgzfStream = Stream(is)
    val blocks = bgzfStream.toList
    blocks.size should be(50)

    blocks(0) should ===(Metadata(     0,  2454,  5650))
    blocks(1) should ===(Metadata(  2454, 25330, 65092))
    blocks(2) should ===(Metadata( 27784, 23602, 64902))

    val compressedStats =
      Stats(
        blocks.map(_.compressedSize)
      )

    check(
      compressedStats,
      """N: 50, μ/σ: 20213.5/4072.1, med/mad: 20218/1497
        | elems: 2454 25330 23602 25052 21680 20314 19775 20396 21533 19644 … 21329 19964 19443 18579 23331 23393 18792 17566 17847 4508
        |sorted: 2454 4508 17566 17733 17847 18038 18216 18271 18579 18632 … 22670 23236 23331 23393 23602 25052 25060 25330 26259 26290
        |   5:	11689.9
        |  10:	17866.1
        |  25:	18986.5
        |  50:	20218
        |  75:	22338
        |  90:	24907
        |  95:	25748.1
      """
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
      """N: 48, μ/σ: 20910.7/2253.3, med/mad: 20271.5/1450
        | elems: 25330 23602 25052 21680 20314 19775 20396 21533 19644 20207 … 25060 21329 19964 19443 18579 23331 23393 18792 17566 17847
        |sorted: 17566 17733 17847 18038 18216 18271 18579 18632 18792 18850 … 22670 23236 23331 23393 23602 25052 25060 25330 26259 26290
        |   5:	17784.3
        |  10:	18198.2
        |  25:	19309.8
        |  50:	20271.5
        |  75:	22382
        |  90:	25052.8
        |  95:	25841.0
      """
    )

    val uncompressedStats =
      Stats(
        blocks.map(_.uncompressedSize)
      )

    check(
      uncompressedStats,
      """N: 50, μ/σ: 62788.1/10728.4, med/mad: 64958.5/178
        | elems: 5650 65092 64902 65248 64839 64643 65187 64752 64893 64960 … 64729 65049 64666 64957 64792 65057 64992 64989 64803 15247
        |sorted: 5650 15247 64630 64643 64666 64677 64685 64704×2 64729 64752 … 65170 65171 65187 65195 65227 65243 65245 65248 65249 65275
        |   5:	42407.7
        |  10:	64667.1
        |  25:	64779.8
        |  50:	64958.5
        |  75:	65139.8
        |  90:	65241.4
        |  95:	65248.5
      """
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
      """N: 48, μ/σ: 64968.9/191.8, med/mad: 64963/173.5
        | elems: 65092 64902 65248 64839 64643 65187 64752 64893 64960 64966 … 64704 64729 65049 64666 64957 64792 65057 64992 64989 64803
        |sorted: 64630 64643 64666 64677 64685 64704×2 64729 64752 64758 64787 … 65170 65171 65187 65195 65227 65243 65245 65248 65249 65275
        |   5:	64653.4
        |  10:	64684.2
        |  25:	64794.8
        |  50:	64963
        |  75:	65141.3
        |  90:	65243.2
        |  95:	65248.6
      """
    )
  }

}
