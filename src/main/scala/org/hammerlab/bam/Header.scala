package org.hammerlab.bam

import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ ByteStream, ByteStreamI }
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.io.{ Buffer, ByteChannel }

case class Header(contigLengths: Map[Int, NumLoci],
                  endPos: Pos,
                  uncompressedSize: Long)

object Header {
  def apply(byteStream: ByteStreamI[_]): Header = {
    val uncompressedByteChannel: ByteChannel = byteStream
    require(
      uncompressedByteChannel.readString(4, includesNull = false) ==
        "BAM\1"
    )

    val headerLength = uncompressedByteChannel.getInt

    /** Skip [[headerLength]] bytes */
    uncompressedByteChannel.skip(headerLength)

    val numReferences = uncompressedByteChannel.getInt

    val contigLengths =
      (for {
        idx ← 0 until numReferences
      } yield {
        val refNameLen = uncompressedByteChannel.getInt
        val nameBuffer = Buffer(refNameLen)  // reference name and length
        uncompressedByteChannel.read(nameBuffer)

        idx →
          NumLoci(uncompressedByteChannel.getInt)
      })
      .toMap

    Header(
      contigLengths,
      byteStream.curPos.get,
      uncompressedByteChannel.position()
    )
  }
}
