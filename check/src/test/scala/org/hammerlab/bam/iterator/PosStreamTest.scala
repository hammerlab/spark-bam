package org.hammerlab.bam.iterator

import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.bgzf.Pos
import org.hammerlab.test.Suite

class PosStreamTest
  extends Suite {

  def checkFirstPositions(implicit stream: PosStreamI[_]): Unit = {
    stream.header.endPos should be(Pos(0, 5650))
    stream.take(10).toList should be(
      Seq(
        Pos(0,  5650),
        Pos(0,  6274),
        Pos(0,  6894),
        Pos(0,  7533),
        Pos(0,  8170),
        Pos(0,  8738),
        Pos(0,  9384),
        Pos(0, 10018),
        Pos(0, 10637),
        Pos(0, 11318)
      )
    )
  }

  val path = bam2

  test("PosStream") {
    implicit val stream = PosStream(path.inputStream)

    checkFirstPositions
  }

  test("SeekablePosStream") {
    implicit val stream = SeekablePosStream(path)

    checkFirstPositions

    stream.seek(Pos(485781, 62305))

    stream.take(10).toList should be(
      Seq(
        Pos(485781, 62305),
        Pos(485781, 62968),
        Pos(485781, 63631),
        Pos(485781, 64294),
        Pos(485781, 64957),
        Pos(503981,   122),
        Pos(503981,   785),
        Pos(503981,  1447),
        Pos(503981,  2111),
        Pos(503981,  2774)
      )
    )

    stream.seek(Pos(503981, 65010))

    stream.take(3).toList should be(
      Seq(
        Pos(503981, 65010),
        Pos(521796,   177),
        Pos(521796,   840)
      )
    )

    stream.seek(Pos(521796, 1510))
    stream.next should be(Pos(521796, 1510))

    stream.seek(Pos(521796, 1510))
    stream.next should be(Pos(521796, 1510))

    stream.seek(Pos(0, 0))
    checkFirstPositions
    stream.seek(Pos(0, 10))
    checkFirstPositions
  }
}
