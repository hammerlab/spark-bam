package org.hammerlab.bam.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.hammerlab.bam.check.eager.Checker
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableByteStream
import org.hammerlab.genomics.reference.NumLoci

object FindRecordStart {

//  def apply(path: Path,
//            conf: Configuration,
//            blockStart: Long,
//            contigLengths: Map[Int, NumLoci],
//            maxReadSize: Int = 100000): Pos = {
//    val fs = path.getFileSystem(conf)
//
//    val uncompressedBytes = SeekableByteStream(fs.open(path))
//
//    try {
//      apply(
//        path,
//        uncompressedBytes,
//        blockStart,
//        contigLengths,
//        maxReadSize
//      )
//    } finally {
//      uncompressedBytes.close()
//    }
//  }

  def apply(path: Path,
            uncompressedBytes: SeekableByteStream,
            blockStart: Long,
            contigLengths: Map[Int, NumLoci],
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
