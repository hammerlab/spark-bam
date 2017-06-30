package org.hammerlab.bgzf.block

import java.nio.channels.FileChannel
import java.nio.file.Paths

import org.hammerlab.io.SeekableByteChannel._
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class MetadataStreamTest
  extends Suite {

  test("metadata") {
    val ch = FileChannel.open(Paths.get(File("5k.bam")))

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
