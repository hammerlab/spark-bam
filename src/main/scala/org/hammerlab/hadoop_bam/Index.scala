package org.hammerlab.hadoop_bam

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder._
import java.nio.channels.FileChannel
import java.nio.file.Paths

import org.apache.hadoop.fs.Path
import org.hammerlab.hadoop_bam.Index.{ Bin, Chunk, Reference }
import org.hammerlab.hadoop_bam.bgzf.VirtualPos
import org.hammerlab.stats.Stats

case class Index(references: Seq[Reference]) {
  @transient lazy val offsets = references.flatMap(_.offsets)

  @transient lazy val chunkStarts =
    for {
      Reference(bins, _) ← references
      Bin(_, chunks) ← bins
      Chunk(start, _) ← chunks
    } yield
      start

  @transient lazy val chunkEnds =
    for {
      Reference(bins, _) ← references
      Bin(_, chunks) ← bins
      Chunk(_, end) ← chunks
    } yield
      end

  @transient lazy val chunkBoundaries = chunkStarts ++ chunkEnds

  @transient lazy val allAddresses = (offsets ++ chunkBoundaries).distinct.sorted

  //@transient lazy val allCPs = allAddresses.map(_.blockPos).distinct.sorted
}

object Index {

  def getDiffStats(offsets: Seq[VirtualPos]): Stats[Long, Int] =
    getDiffStats(
      offsets
        .map(_.blockPos)
        .toArray
        .sorted
    )

  def getDiffStats(compressedPositions: Array[Long]): Stats[Long, Int] = {
    val diffs = compressedPositions.sliding(2).map(l => l(1) - l(0)).toArray
    Stats(diffs)
  }

  case class Reference(bins: Seq[Bin], offsets: Seq[VirtualPos])

  case class Bin(id: Int, chunks: Seq[Chunk])

  case class Chunk(start: VirtualPos, end: VirtualPos)

  def apply(path: Path): Index =
    Index(
      {
        val ch = FileChannel.open(Paths.get(path.toUri))

        val buf4 = ByteBuffer.allocate(4).order(LITTLE_ENDIAN)
        val buf8 = ByteBuffer.allocate(8).order(LITTLE_ENDIAN)

        ch.read(buf4)
        if (buf4.array().map(_.toChar).mkString("") != "BAI\1") {
          throw new IOException(s"Bad BAI magic: ${buf4.array()}")
        }

        def readInt: Int = {
          buf4.clear()
          val numBytes = ch.read(buf4)
          if (numBytes < 4) {
            throw new IOException(s"Expected 4 bytes, read $numBytes")
          }
          buf4.position(0)
          buf4.getInt()
        }

        def readLong: Long = {
          buf8.clear()
          val numBytes = ch.read(buf8)
          if (numBytes < 8) {
            throw new IOException(s"Expected 8 bytes, read $numBytes")
          }
          buf8.position(0)
          buf8.getLong()
        }

        /** Read an integer, then read that many instances of a given type T. */
        def seq[T](fn: () ⇒ T): Seq[T] = {
          val num = readInt
          for {
            _ ← 0 until num
          } yield
            fn()
        }

        def readChunk() =
          Chunk(
            VirtualPos(readLong),
            VirtualPos(readLong)
          )

        def readChunks = seq(readChunk)

        def readBin() = Bin(readInt, readChunks)
        def readBins = seq(readBin)

        def readOffset() = VirtualPos(readLong)
        def readOffsets = seq(readOffset)

        def readReference() =
          Reference(
            readBins,
            readOffsets
          )

        seq(readReference)
      }
    )
}
