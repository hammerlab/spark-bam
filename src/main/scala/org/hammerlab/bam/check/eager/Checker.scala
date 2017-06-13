package org.hammerlab.bam.check.eager

import java.io.IOException

import org.hammerlab.bam.check.Checker.allowedReadNameChars
import org.hammerlab.bam.check
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.block.SeekableByteStream

/**
 * [[check.Checker]] implementation that emits a [[Boolean]] at each [[org.hammerlab.bgzf.Pos]] indicating whether it is
 * a read-record boundary.
 */
case class Checker(uncompressedStream: SeekableByteStream,
                   contigLengths: ContigLengths)
  extends check.Checker[Boolean] {

  override def tooFewFixedBlockBytes: Boolean = false

  override def apply(remainingBytes: Int): Boolean = {

    if (getRefPosError().isDefined)
      return false

    val readNameLength = buf.getInt & 0xff
    readNameLength match {
      case 0 | 1 ⇒
        return false
      case _ ⇒
    }

    val numCigarOps = buf.getInt & 0xffff
    val numCigarBytes = 4 * numCigarOps

    val seqLen = buf.getInt

    val numSeqAndQualBytes = (seqLen + 1) / 2 + seqLen

    if(remainingBytes < 32 + readNameLength + numCigarBytes + numSeqAndQualBytes)
      return false

    if (getRefPosError().isDefined)
      return false

    buf.getInt  // unused: template length

    try {
      readNameBuffer.position(0)
      readNameBuffer.limit(readNameLength)
      ch.read(readNameBuffer)
      val readNameBytes = readNameBuffer.array().view.slice(0, readNameLength)

      if (readNameBytes.last != 0)
        return false
      else if (
        readNameBytes
          .view
          .slice(0, readNameLength - 1)
          .exists(byte ⇒ !allowedReadNameChars(byte.toChar))
      )
        return false

      try {
        if (
          (0 until numCigarOps)
          .exists {
            _ ⇒
              (ch.getInt & 0xf) > 8
          }
        )
          return false
      } catch {
        case _: IOException ⇒
          return false
      }

    } catch {
      case _: IOException ⇒
        return false
    }

    true
  }
}
