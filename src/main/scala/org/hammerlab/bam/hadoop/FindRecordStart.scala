package org.hammerlab.bam.hadoop

import org.apache.hadoop.fs.Path
import org.hammerlab.bam.check.eager.Checker
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableByteStream

object FindRecordStart {

  def apply(path: Path,
            uncompressedBytes: SeekableByteStream,
            blockStart: Long,
            contigLengths: ContigLengths,
            maxReadSize: Int = 100000): Pos = {

    uncompressedBytes.seek(Pos(blockStart, 0))

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
            return pos
          }
          uncompressedBytes.seek(pos)  // go back to this failed position
          uncompressedBytes.next()     // move over by 1 byte
        case None ⇒
          throw NoReadFoundException(path, blockStart, maxReadSize)
      }
      idx += 1
    }

    throw NoReadFoundException(path, blockStart, maxReadSize)
  }
}

case class NoReadFoundException(path: Path,
                                blockStart: Long,
                                maxReadSize: Int)
  extends Exception(
    s"Failed to find a valid read-start in $maxReadSize attempts from $path offset $blockStart"
  )
