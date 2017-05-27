package org.hammerlab.hadoop_bam.bgzf.block

import java.nio.channels.FileChannel
import java.nio.file.Paths

import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class MetadataStreamTest
  extends Suite {

  test("metadata") {
    val ch = FileChannel.open(Paths.get(File("2.bam")))
    MetadataStream(ch).size should be(13380)
    ch.position(0)
    MetadataStream(ch).take(10).toList should be(
      List(
        Metadata(     0, 65498, 15071),
        Metadata( 15071, 65498, 25969),
        Metadata( 41040, 65498, 26000),
        Metadata( 67040, 65498, 26842),
        Metadata( 93882, 65498, 25118),
        Metadata(119000, 65498, 25017),
        Metadata(144017, 65498, 25814),
        Metadata(169831, 65498, 24814),
        Metadata(194645, 65498, 24059),
        Metadata(218704, 65498, 24597)
      )
    )
  }
}
