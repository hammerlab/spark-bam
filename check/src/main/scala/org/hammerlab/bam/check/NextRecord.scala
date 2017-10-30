package org.hammerlab.bam.check

import hammerlab.show._
import htsjdk.samtools.SAMRecord

case class NextRecord(record: SAMRecord, delta: Int)

object NextRecord {
  implicit def makeShow(implicit showRecord: Show[SAMRecord]): Show[NextRecord] =
    show {
      case NextRecord(record, delta) ⇒
        show"$delta before $record"
    }
}
