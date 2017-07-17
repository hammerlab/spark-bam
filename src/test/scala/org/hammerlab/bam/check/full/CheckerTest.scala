package org.hammerlab.bam.check.full

import org.hammerlab.bam.check.full.error.Flags
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.io.SeekableByteChannel
import org.hammerlab.resources.bam5k
import org.hammerlab.test.Suite

class CheckerTest
  extends Suite {

  test("EoF") {
    val path = bam5k.path
    val uncompressedBytes =
      SeekableUncompressedBytes(
        SeekableByteChannel(path)
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
