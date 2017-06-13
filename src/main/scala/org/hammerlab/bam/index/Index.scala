package org.hammerlab.bam.index

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.hammerlab.bam.index.Index.{ Bin, Chunk, Reference }
import org.hammerlab.bam.index.Read._
import org.hammerlab.bgzf.{ EstimatedCompressionRatio, Pos }
import org.hammerlab.io.ByteChannel

case class Index(references: Seq[Reference]) {
  @transient lazy val offsets = references.flatMap(_.offsets)

  @transient lazy val chunkStarts =
    for {
      Reference(bins, _, _) ← references
      Bin(_, chunks) ← bins
      Chunk(start, _) ← chunks
    } yield
      start

  @transient lazy val chunkEnds =
    for {
      Reference(bins, _, _) ← references
      Bin(_, chunks) ← bins
      Chunk(_, end) ← chunks
    } yield
      end

  @transient lazy val chunks =
    for {
      Reference(bins, _, _) ← references
      Bin(_, chunks) ← bins
      chunk ← chunks
    } yield
      chunk

  @transient lazy val chunkBoundaries = chunkStarts ++ chunkEnds

  @transient lazy val allAddresses = (offsets ++ chunkBoundaries).distinct.sorted
}

object Index {

  case class Reference(bins: Seq[Bin],
                       offsets: Seq[Pos],
                       metadata: Option[Metadata])

  case class Bin(id: Long,
                 chunks: Seq[Chunk])

  case class Chunk(start: Pos,
                   end: Pos) {
    def size(implicit estimatedCompressionRatio: EstimatedCompressionRatio): Double =
      end - start
  }

  case class Metadata(unmappedBegin: Pos,
                      unmappedEnd: Pos,
                      numMapped: Long,
                      numUnmapped: Long)

  object Chunk {
    implicit def fromHTSJDKChunk(chunk: htsjdk.samtools.Chunk): Chunk =
      Chunk(
        Pos(chunk.getChunkStart),
        Pos(chunk.getChunkEnd)
      )
  }

  def apply(path: Path, conf: Configuration): Index =
    Index(
      {
        implicit val ch: ByteChannel = path.getFileSystem(conf).open(path)

        if (ch.readString(4, includesNull = false) != "BAI\1")
          throw new IOException(s"Bad BAI magic in $path")

        read[Seq[Reference]]
      }
    )

  val METADATA_BIN_ID = 37450
}
