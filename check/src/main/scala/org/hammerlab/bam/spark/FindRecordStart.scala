package org.hammerlab.bam.spark

import org.hammerlab.bam.check.{ MaxReadSize, ReadsToCheck, eager }
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.paths.Path

object FindRecordStart {

  def apply(path: Path,
            start: Long)(
      implicit
      uncompressedBytes: SeekableUncompressedBytes,
      contigLengths: ContigLengths,
      readsToCheck: ReadsToCheck,
      maxReadSize: MaxReadSize): Pos =
    withDelta(
      Pos(start, 0)
    )
    .map(_._1)
    .getOrElse(
      throw NoReadFoundException(
        path,
        start,
        maxReadSize
      )
    )

  def withDelta(start: Pos)(
      implicit
      uncompressedBytes: SeekableUncompressedBytes,
      contigLengths: ContigLengths,
      readsToCheck: ReadsToCheck,
      maxReadSize: MaxReadSize
  ): Option[(Pos, Int)] = {

    uncompressedBytes.seek(start)

    val checker = eager.Checker(uncompressedBytes, contigLengths, readsToCheck)

    var idx = 0
    while (idx < maxReadSize.n) {
      uncompressedBytes.curPos match {
        case Some(pos) ⇒
          if (checker(pos)) {
            return Some(pos → idx)
          }

          uncompressedBytes.seek(pos)  // go back to this failed position

          if (!uncompressedBytes.hasNext)
            return None
          
          uncompressedBytes.next()     // move over by 1 byte
        case None ⇒
          return None
      }
      idx += 1
    }

    None
  }
}

case class NoReadFoundException(path: Path,
                                start: Long,
                                maxReadSize: MaxReadSize)
  extends Exception(
    s"Failed to find a valid read-start in $maxReadSize attempts in $path from $start"
  )
