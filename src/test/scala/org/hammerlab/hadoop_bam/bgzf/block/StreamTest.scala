package org.hammerlab.hadoop_bam.bgzf.block

import java.io.FileInputStream

import org.hammerlab.stats.Stats
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class StreamTest
  extends Suite {

  test("test5k.bam") {
    val is = new FileInputStream(File("test5k.bam"))
    val bgzfStream = new Stream(is)
    val blocks = bgzfStream.toList
    blocks.size should be(50)
    blocks(0).start should be(0)
    blocks(0).compressedSize should be(2454)
    blocks(0).uncompressedSize should be(5650)

    blocks(1).start should be(2454)
    blocks(1).compressedSize should be(25330)
    blocks(1).uncompressedSize should be(65092)

    blocks(2).start should be(27784)
    blocks(2).compressedSize should be(23602)
    blocks(2).uncompressedSize should be(64902)

//    println(Stats(blocks.map(_.compressedSize)))
//    println(Stats(blocks.map(_.compressedSize).drop(1).dropRight(1)))
//
//    println(Stats(blocks.map(_.uncompressedSize)))
//    println(Stats(blocks.map(_.uncompressedSize).drop(1).dropRight(1)))

//    blocks.map(_.compressedSize) should be(Seq(2454, 25330, 23602, 25052, 21680, 20314, 19775, 20396, 21533, 19644, 20207, 21221, 21073, 22404, 26290, 20229, 19607, 19032, 18216, 18038, 18271, 17733, 19387, 20017, 20170, 21993, 22670, 22316, 23236, 26259, 21750, 18632, 20928, 19765, 18850, 19675, 19284, 21320, 22510, 25060, 21329, 19964, 19443, 18579, 23331, 23393, 18792, 17566, 17847, 4508))
//    blocks.map(_.uncompressedSize) should be(Seq())
  }

}
