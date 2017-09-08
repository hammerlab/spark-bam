package org.hammerlab.bgzf.block

import java.nio.channels.FileChannel

import org.hammerlab.bam.test.resources.bam5k
import org.hammerlab.channel.SeekableByteChannel._
import org.hammerlab.test.Suite

class MetadataStreamTest
  extends Suite {

  test("metadata") {
    val ch = FileChannel.open(bam5k)

    MetadataStream(ch)
      .take(10)
      .toList should be(
      List(
        Metadata(     0,  2454,  5650),
        Metadata(  2454, 25330, 65092),
        Metadata( 27784, 23602, 64902),
        Metadata( 51386, 25052, 65248),
        Metadata( 76438, 21680, 64839),
        Metadata( 98118, 20314, 64643),
        Metadata(118432, 19775, 65187),
        Metadata(138207, 20396, 64752),
        Metadata(158603, 21533, 64893),
        Metadata(180136, 19644, 64960)
      )
    )

    ch.position(0)
    val stream = MetadataStream(ch)

    stream.size should be(50)

    ch.isOpen should be(true)
    stream.close()
    ch.isOpen should be(false)
  }
}