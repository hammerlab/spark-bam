package org.hammerlab.bam.check.full

import org.apache.hadoop.conf.Configuration
import org.hammerlab.bam.check.full.error.Flags
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableByteStream
import org.hammerlab.hadoop.Path
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class CheckerTest
  extends Suite {

  test("EoF") {
    val conf = new Configuration
    val path = Path(File("5k.bam").uri)
    val uncompressedBytes =
      SeekableByteStream(
        path
          .getFileSystem(conf)
          .open(path)
      )

    val checker =
      Checker(
        uncompressedBytes,
        Map()
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
