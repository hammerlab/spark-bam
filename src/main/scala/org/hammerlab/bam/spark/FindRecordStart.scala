package org.hammerlab.bam.spark

import org.hammerlab.bam.check.eager.Checker
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.paths.Path

object FindRecordStart {

  def apply(path: Path,
            uncompressedBytes: SeekableUncompressedBytes,
            start: Long,
            contigLengths: ContigLengths,
            maxReadSize: Int = 100000): Pos =
    withDelta(
      uncompressedBytes,
      Pos(start, 0),
      contigLengths,
      maxReadSize
    )
    .map(_._1)
    .getOrElse(
      throw NoReadFoundException(
        path,
        start,
        maxReadSize
      )
    )

  def withDelta(uncompressedBytes: SeekableUncompressedBytes,
                start: Pos,
                contigLengths: ContigLengths,
                maxReadSize: Int = 100000): Option[(Pos, Int)] = {

    uncompressedBytes.seek(start)

    val checker =
      Checker(
        uncompressedBytes,
        contigLengths
      )

    var idx = 0
    while (idx < maxReadSize) {
      uncompressedBytes.curPos match {
        case Some(pos) ⇒
          if (checker()) {
            return Some(pos → idx)
          }
          uncompressedBytes.seek(pos)  // go back to this failed position
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
                                maxReadSize: Int)
  extends Exception(
    s"Failed to find a valid read-start in $maxReadSize attempts in $path from $start"
  )
