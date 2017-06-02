package org.hammerlab.bgzf.block

import java.io.FileInputStream
import java.nio.channels.FileChannel

import org.hammerlab.stats.Stats
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class StreamTest
  extends Suite {

  def check(stats: Stats[Int, Int], expectedStr: String): Unit = {
    stats.toString should be(
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

    val ch = FileChannel.open(File("5k.bam").path)
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
    val is = new FileInputStream(File("5k.bam"))
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
      """
        |num:	50,	mean:	20213.5,	stddev:	4072.1,	mad:	1497
        |elems:	2454, 25330, 23602, 25052, 21680, 20314, 19775, 20396, 21533, 19644, …, 21329, 19964, 19443, 18579, 23331, 23393, 18792, 17566, 17847, 4508
        |sorted:	2454, 4508, 17566, 17733, 17847, 18038, 18216, 18271, 18579, 18632, …, 22670, 23236, 23331, 23393, 23602, 25052, 25060, 25330, 26259, 26290
        |5:	17657.9
        |10:	17866.1
        |25:	19221
        |50:	20218
        |75:	22235.3
        |90:	23747
        |95:	25208.5
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
      """
        |num:	48,	mean:	20910.7,	stddev:	2253.3,	mad:	1450
        |elems:	25330, 23602, 25052, 21680, 20314, 19775, 20396, 21533, 19644, 20207, …, 25060, 21329, 19964, 19443, 18579, 23331, 23393, 18792, 17566, 17847
        |sorted:	17566, 17733, 17847, 18038, 18216, 18271, 18579, 18632, 18792, 18850, …, 22670, 23236, 23331, 23393, 23602, 25052, 25060, 25330, 26259, 26290
        |5:	17971.2
        |10:	18232.5
        |25:	19309.8
        |50:	20271.5
        |75:	22338
        |90:	24037
        |95:	25235.5
      """.stripMargin
    )

    val uncompressedStats =
      Stats(
        blocks.map(_.uncompressedSize)
      )

    check(
      uncompressedStats,
      """
        |num:	50,	mean:	62788.1,	stddev:	10728.4,	mad:	178
        |elems:	5650, 65092, 64902, 65248, 64839, 64643, 65187, 64752, 64893, 64960, …, 64729, 65049, 64666, 64957, 64792, 65057, 64992, 64989, 64803, 15247
        |sorted:	5650, 15247, 64630, 64643, 64666, 64677, 64685, 64704×2, 64729, 64752, …, 65170, 65171, 65187, 65195, 65227, 65243, 65245, 65248, 65249, 65275
        |5:	64637.1
        |10:	64667.1
        |25:	64790.8
        |50:	64958.5
        |75:	65137.8
        |90:	65228.6
        |95:	65246.6
      """.stripMargin
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
      """
        |num:	48,	mean:	64968.9,	stddev:	191.8,	mad:	173.5
        |elems:	65092, 64902, 65248, 64839, 64643, 65187, 64752, 64893, 64960, 64966, …, 64704, 64729, 65049, 64666, 64957, 64792, 65057, 64992, 64989, 64803
        |sorted:	64630, 64643, 64666, 64677, 64685, 64704×2, 64729, 64752, 64758, 64787, …, 65170, 65171, 65187, 65195, 65227, 65243, 65245, 65248, 65249, 65275
        |5:	64673.2
        |10:	64690.7
        |25:	64794.8
        |50:	64963
        |75:	65139.8
        |90:	65231.8
        |95:	65247.0
      """.stripMargin
    )
  }

}
