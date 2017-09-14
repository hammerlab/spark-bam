package org.hammerlab.bgzf.block

import java.nio.channels.FileChannel

import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.channel.SeekableByteChannel._
import org.hammerlab.test.Suite

class MetadataStreamTest
  extends Suite {

  test("metadata") {
    val ch = FileChannel.open(bam2)

    MetadataStream(ch)
      .take(10)
      .toList should be(
      List(
        Metadata(     0, 26169, 65498),
        Metadata( 26169, 24080, 65498),
        Metadata( 50249, 25542, 65498),
        Metadata( 75791, 22308, 65498),
        Metadata( 98099, 20688, 65498),
        Metadata(118787, 19943, 65498),
        Metadata(138730, 20818, 65498),
        Metadata(159548, 21957, 65498),
        Metadata(181505, 19888, 65498),
        Metadata(201393, 20517, 65498)
      )
    )

    ch.position(0)
    val stream = MetadataStream(ch)

    stream.size should be(30)

    ch.isOpen should be(true)
    stream.close()
    ch.isOpen should be(false)
  }
}
