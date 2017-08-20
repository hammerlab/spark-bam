package org.hammerlab.bam.check.full

import org.hammerlab.bam.check.Checker.{ ReadsToCheck, default }
import org.hammerlab.bam.check.full.error.Flags
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

  def check(file: File, pos: Pos, expected: Option[Flags]): Unit = {
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

    val actual = checker(pos)

    import cats.syntax.all._
    import org.hammerlab.io.show._

    println(actual.show)

    actual should be(expected)
  }

  test("fn") {
    check(
      File("12896294.bam"),
      Pos(12100265, 37092),
      None
    )
  }

  test("EoF") {
    check(
      bam5k,
      Pos(1006167, 15243),
      Some(
        Flags(
          tooFewFixedBlockBytes = true,
          None, None, None, None, false,
          readsBeforeError = 0
        )
      )
    )
  }
}
