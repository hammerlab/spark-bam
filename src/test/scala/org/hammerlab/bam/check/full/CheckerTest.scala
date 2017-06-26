package org.hammerlab.bam.check.full

import org.apache.hadoop.conf.Configuration
import org.hammerlab.bam.check.full.error.Flags
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.hadoop.Path
import org.hammerlab.io.SeekableByteChannel.SeekableHadoopByteChannel
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class CheckerTest
  extends Suite {

  test("EoF") {
    implicit val conf = new Configuration
    val path = Path(File("5k.bam").uri)
    val uncompressedBytes =
      SeekableUncompressedBytes(
        SeekableHadoopByteChannel(path)
      )

    val checker =
      Checker(
        uncompressedBytes,
        ContigLengths(Nil)
      )

    uncompressedBytes.seek(Pos(1006167, 15243))

    checker() should be(
      Some(
        Flags(
          tooFewFixedBlockBytes = true,
          None, None, None, None, false
        )
      )
    )
  }
}