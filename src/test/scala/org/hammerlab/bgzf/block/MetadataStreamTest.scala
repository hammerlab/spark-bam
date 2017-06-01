package org.hammerlab.bgzf.block

import java.nio.channels.FileChannel
import java.nio.file.Paths

import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class MetadataStreamTest
  extends Suite {

  test("metadata") {
    val ch = FileChannel.open(Paths.get(File("5k.bam")))

    MetadataStream(
      ch,
      includeEmptyFinalBlock = true,
      closeStream = false
    )
    .size should be(
      51
    )

    ch.position(0)
    MetadataStream(ch)
      .take(10)
      .toList should be(
      List(
        Metadata(     0,  5650,  2454),
        Metadata(  2454, 65092, 25330),
        Metadata( 27784, 64902, 23602),
        Metadata( 51386, 65248, 25052),
        Metadata( 76438, 64839, 21680),
        Metadata( 98118, 64643, 20314),
        Metadata(118432, 65187, 19775),
        Metadata(138207, 64752, 20396),
        Metadata(158603, 64893, 21533),
        Metadata(180136, 64960, 19644)
      )
    )

    ch.position(0)
    MetadataStream(
      ch,
      includeEmptyFinalBlock = false
    )
    .size should be(
      50
    )
  }
}
