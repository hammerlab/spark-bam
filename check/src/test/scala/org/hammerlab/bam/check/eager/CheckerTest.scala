package org.hammerlab.bam.check.eager

import org.hammerlab.bam.check.Checker.default
import org.hammerlab.bam.check.ReadsToCheck
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.hadoop.Configuration
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class CheckerTest
  extends Suite {

  def check(file: File, pos: Pos, expected: Boolean): Unit = {
    val path = file.path
    val uncompressedBytes =
      SeekableUncompressedBytes(
        SeekableByteChannel(path)
      )

    implicit val conf = Configuration()

    val checker =
      Checker(
        uncompressedBytes,
        ContigLengths(path),
        readsToCheck = default[ReadsToCheck]
      )

    checker(pos) should be(
      expected
    )
  }
}
