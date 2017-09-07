package org.hammerlab.bam.check

import cats.Show
import cats.implicits.catsStdShowForInt
import cats.Show.show
import cats.syntax.all._
import htsjdk.samtools.SAMRecord

case class NextRecord(record: SAMRecord, delta: Int)

object NextRecord {
  implicit def makeShow(implicit showRecord: Show[SAMRecord]): Show[NextRecord] =
    show {
      case NextRecord(record, delta) ⇒
        show"$delta before $record"
    }
}
