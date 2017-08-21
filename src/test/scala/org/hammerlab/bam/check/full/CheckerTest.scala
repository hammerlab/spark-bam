package org.hammerlab.bam.check.full

import org.hammerlab.bam.check.Checker.{ ReadsToCheck, default }
import org.hammerlab.bam.check.full.error.{ Flags, Result, Success }
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.hadoop.Configuration
import org.hammerlab.resources.bam5k
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class CheckerTest
  extends Suite {

  def check(file: File, pos: Pos, expected: Result): Unit = {
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
        default[ReadsToCheck]
      )

    checker(pos) should be(expected)
  }

  test("empty mapped seq / cigar") {
    check(
      File("prefix.bam"),
      Pos(12100265, 37092),
      Flags(
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        emptyMappedCigar = true,
        emptyMappedSeq = true,
        false,
        0
      )
    )
  }

  test("EoF") {
    check(
      bam5k,
      Pos(1006167, 15243),
      Flags(
        tooFewFixedBlockBytes = true,
        None, None, None, None, false,
        readsBeforeError = 0
      )
    )
  }
}
