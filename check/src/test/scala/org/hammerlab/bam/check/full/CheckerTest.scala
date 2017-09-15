package org.hammerlab.bam.check.full

import java.nio.channels.FileChannel

import org.hammerlab.bam.check.Checker.{ ReadsToCheck, default }
import org.hammerlab.bam.check.full.error.{ Flags, InvalidCigarOp, NoReadName, Result, Success }
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.channel.SeekableByteChannel.ChannelByteChannel
import org.hammerlab.hadoop.Configuration
import org.hammerlab.paths.Path
import org.hammerlab.test.Suite

class CheckerTest
  extends Suite {

  def check(path: Path, pos: Pos, expected: Result): Unit = {
    val uncompressedBytes =
      SeekableUncompressedBytes(
        ChannelByteChannel(FileChannel.open(path))
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

  test("true positive") {
    check(
      bam2,
      Pos(439897, 52186),
      Success(10)
    )
  }

  test("2 checks fail in header") {
    check(
      bam2,
      Pos(0, 5649),
      Flags(
        tooFewFixedBlockBytes = false,
        readPosError = None,
        nextReadPosError = None,
        readNameError = Some(NoReadName),
        cigarOpsError = Some(InvalidCigarOp),
        tooFewRemainingBytesImplied = false,
        readsBeforeError = 0
      )
    )
  }

  test("EoF") {
    check(
      bam2,
      Pos(1006167, 15243),
      Flags(
        tooFewFixedBlockBytes = true,
        None, None, None, None, false,
        readsBeforeError = 0
      )
    )
  }
}
