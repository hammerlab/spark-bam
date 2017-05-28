package org.hammerlab.bam.check

import java.nio.ByteBuffer
import java.nio.ByteOrder.LITTLE_ENDIAN

import org.hammerlab.bgzf.Pos
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.iterator.{ HeadOptionIterator, SimpleBufferedIterator }

case class PosCallIterator(block: Long,
                           usize: Int,
                           offsets: Vector[Int],
                           nextPosOpt: Option[Pos],
                           bytes: Array[Byte],
                           contigLengths: Map[Int, NumLoci])
  extends SimpleBufferedIterator[(Pos, Call)] {

  val buf = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN)

  val expectedPoss =
    (
      offsets
      .iterator
      .map(offset ⇒ Pos(block, offset)) ++
        nextPosOpt.iterator
      )
    .buffered

  var up = 0
  override protected def _advance: Option[(Pos, Call)] =
    if (up >= usize)
      None
    else {
      val pos = Pos(block, up)
      buf.position(up)

      while (expectedPoss.hasNext && pos > expectedPoss.head) {
        expectedPoss.next
      }

      val expectPositive = expectedPoss.headOption.contains(pos)

      Some(
        pos → (
          Guesser.guess(buf, contigLengths) match {
            case Some(error) ⇒
              if (expectPositive) {
                FalseNegative(error)
              } else {
                TrueNegative(error)
              }
            case None ⇒
              if (expectPositive)
                TruePositive
              else
                FalsePositive
          }
          )
      )
    }

  override protected def postNext(): Unit = {
    up += 1
  }
}
