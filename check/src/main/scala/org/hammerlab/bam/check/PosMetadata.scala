package org.hammerlab.bam.check

import cats.Show
import cats.syntax.all._
import cats.Show.show
import htsjdk.samtools.SAMRecord
import org.apache.spark.broadcast.Broadcast
import org.hammerlab.bam.check.full.error.Flags
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bam.iterator.RecordStream
import org.hammerlab.bam.spark.FindRecordStart
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes

case class PosMetadata(pos: Pos,
                       recordOpt: Option[NextRecord],
                       flags: Flags)

object PosMetadata {

  implicit def defaultShow(implicit showRecord: Show[SAMRecord]): Show[PosMetadata] =
    show {
      case PosMetadata(pos, recordOpt, flags) ⇒
        show"$pos:\t$recordOpt. Failing checks: $flags"
    }

  implicit def showNextRecordOpt(implicit showNextRecord: Show[NextRecord]): Show[Option[NextRecord]] =
    show {
      case Some(nextRecord) ⇒ nextRecord.show
      case None ⇒ "no next record"
    }

  def recordPos(record: SAMRecord)(implicit contigLengthsBroadcast: Broadcast[ContigLengths]): String =
    s"${contigLengthsBroadcast.value.apply(record.getReferenceIndex)._1}:${record.getStart}"

  implicit def showRecord(implicit contigLengthsBroadcast: Broadcast[ContigLengths]): Show[SAMRecord] =
    show {
      record ⇒
        record
          .toString
          .dropRight(1) +  // remove trailing period
            (
              // Append info about mapped/placed location
              if (
                record.getReadUnmappedFlag &&
                  record.getStart >= 0 &&
                  record.getReferenceIndex >= 0 &&
                  record.getReferenceIndex < contigLengthsBroadcast.value.size
              )
                s" (placed at ${recordPos(record)})"
              else if (!record.getReadUnmappedFlag)
                s" @ ${recordPos(record)}"
              else
                ""
            )
    }

  def apply(pos: Pos,
            flags: Flags)(
      implicit
      uncompressedBytes: SeekableUncompressedBytes,
      header: Broadcast[Header],
      readsToCheck: ReadsToCheck,
      maxReadSize: MaxReadSize
  ): PosMetadata = {
    implicit val contigLengths = header.value.contigLengths
    PosMetadata(
      pos,
      {
        FindRecordStart
          .withDelta(pos)
          .map {
            case (nextRecordPos, delta) ⇒

              uncompressedBytes.seek(nextRecordPos)

              NextRecord(
                RecordStream(
                  uncompressedBytes,
                  header.value
                )
                .next()
                ._2,
                delta
              )
          }
      },
      flags
    )
  }
}
