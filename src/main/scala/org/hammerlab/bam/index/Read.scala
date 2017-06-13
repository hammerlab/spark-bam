package org.hammerlab.bam.index

import org.hammerlab.bam.index.Index.{ Bin, Chunk, METADATA_BIN_ID, Metadata, Reference }
import org.hammerlab.bgzf.Pos
import org.hammerlab.io.{ Buffer, ByteChannel }

import scala.collection.mutable.ArrayBuffer

trait Read[T] {
  def apply(implicit ch: ByteChannel): T
}

object Read {
  def read[T](implicit ch: ByteChannel, readT: Read[T]): T =
    readT.apply

  implicit def readSeq[T: Read]: Read[Seq[T]] =
    new Read[Seq[T]] {
      override def apply(implicit ch: ByteChannel): Seq[T] = {
        val num = ch.getInt
        for {
          _ ← 0 until num
        } yield
          read[T]
      }
    }

  implicit object ReadLong extends Read[Long] {
    val buf8 = Buffer(8)

    override def apply(implicit ch: ByteChannel): Long = {
      buf8.clear()
      ch.read(buf8)
      buf8.position(0)
      buf8.getLong()
    }
  }

  implicit object ReadPos extends Read[Pos] {
    override def apply(implicit ch: ByteChannel): Pos =
      Pos(read[Long])
  }

  implicit object ReadChunk extends Read[Chunk] {
    override def apply(implicit ch: ByteChannel): Chunk =
      Chunk(
        read[Pos],
        read[Pos]
      )
  }

  implicit object ReadBin extends Read[Either[Bin, Metadata]] {
    override def apply(implicit ch: ByteChannel): Either[Bin, Metadata] = {
      val id = ch.getInt
      if (id == METADATA_BIN_ID) {
        val numChunks = ch.getInt
        if (numChunks != 2) {
          throw new IllegalStateException(
            s"Metadata bin $id should have 2 chunks, found $numChunks"
          )
        }
        Right(
          Metadata(
            read[Pos],
            read[Pos],
            read[Long],
            read[Long]
          )
        )
      } else
        Left(
          Bin(
            id,
            read[Seq[Chunk]]
          )
        )
    }
  }

  implicit object ReadBins extends Read[(Seq[Bin], Option[Metadata])] {
    override def apply(implicit ch: ByteChannel): (Seq[Bin], Option[Metadata]) = {
      val numBins = ch.getInt
      var metadataOpt: Option[Metadata] = None
      val bins = ArrayBuffer[Bin]()
      for {
        _ ← 0 until numBins
      } {
        read[Either[Bin, Metadata]] match {
          case Left(bin) ⇒
            bins += bin
          case Right(metadata) ⇒
            metadataOpt match {
              case None ⇒
                metadataOpt = Some(metadata)
              case Some(existing) ⇒
                throw new IllegalStateException(
                  s"Found two metadata: $existing, $metadata"
                )
            }
        }
      }
      bins → metadataOpt
    }
  }

  implicit object ReadReference extends Read[Reference] {
    override def apply(implicit ch: ByteChannel): Reference = {
      val (bins, metadata) = read[(Seq[Bin], Option[Metadata])]
      val offsets = read[Seq[Pos]]
      Reference(bins, offsets, metadata)
    }
  }
}
