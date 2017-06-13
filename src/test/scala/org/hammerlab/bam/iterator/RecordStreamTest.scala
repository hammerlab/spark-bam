package org.hammerlab.bam.iterator

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.conf.Configuration
import org.hammerlab.bgzf.Pos
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class RecordStreamTest
  extends Suite {

  sealed trait StreamOp

  case class Record(pos: Pos, start: Int, name: String) extends StreamOp
  implicit def makeRecord(t: (Pos, Int, String)): Record = Record(t._1, t._2, t._3)

  case class Drop(n: Int) extends StreamOp

  // Testing shorthand: convert int -> int to VirtualPos
  implicit class IntsToPos(val blockPos: Int) {
    def →(offset: Int) =
      Pos(blockPos, offset)
  }

  def recordTuple(t: (Pos, SAMRecord)): (Pos, Int, String, String) =
    (t._1, t._2.getAlignmentStart, t._2.getReadName, t._2.getReferenceName)

  def check(ops: StreamOp*)(implicit rs: RecordStreamI[_]): Unit =
    ops.foreach {
      case Drop(n) ⇒ rs.drop(n)
      case Record(pos, start, name) ⇒
        rs.curPos should be(Some(pos))
        check(pos, start, name)
    }

  def check(expectedPos: Pos, start: Int, name: String)(implicit rs: RecordStreamI[_]): Unit = {
    val (pos, rec) = rs.next
    recordTuple(pos, rec) should be(
      (expectedPos, start, name, "1")
    )
  }

  def checkFirstRecords(implicit rs: RecordStreamI[_]): Unit = {
    rs.headerEndPos should be(Pos(2454, 0))
    check(
      ( 2454 →     0, 10001, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724"),
      ( 2454 →   624, 10009, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724"),
      ( 2454 →  1244, 10048, "HWI-ST807:461:C2P0JACXX:4:1304:9505:89866"),
      ( 2454 →  1883, 10335, "HWI-ST807:461:C2P0JACXX:4:2311:6431:65669"),
      ( 2454 →  2520, 10363, "HWI-ST807:461:C2P0JACXX:4:1305:2342:51860"),
      ( 2454 →  3088, 10363, "HWI-ST807:461:C2P0JACXX:4:1305:2342:51860"),
      ( 2454 →  3734, 10368, "HWI-ST807:461:C2P0JACXX:4:1304:9505:89866"),
      ( 2454 →  4368, 10458, "HWI-ST807:461:C2P0JACXX:4:2311:6431:65669"),
      ( 2454 →  4987, 11648, "HWI-ST807:461:C2P0JACXX:4:1107:13461:64844"),
      ( 2454 →  5668, 11687, "HWI-ST807:461:C2P0JACXX:4:2203:17157:59976"),
      Drop(20),
      ( 2454 → 18684, 11911, "HWI-ST807:461:C2P0JACXX:4:2302:17669:14421"),
      Drop(20),
      ( 2454 → 32162, 12123, "HWI-ST807:461:C2P0JACXX:4:2101:17257:40389"),
      Drop(20),
      ( 2454 → 45238, 12298, "HWI-ST807:461:C2P0JACXX:4:1312:6062:40537"),
      Drop(20),
      ( 2454 → 58258, 12572, "HWI-ST807:461:C2P0JACXX:4:1205:8857:43215"),
      Drop(9),
      ( 2454 → 64473, 12656, "HWI-ST807:461:C2P0JACXX:4:2103:9923:17201"),
      (27784 →     0, 12676, "HWI-ST807:461:C2P0JACXX:4:2310:1258:47908"),
      (27784 →   664, 12682, "HWI-ST807:461:C2P0JACXX:4:2302:19124:35915")
    )
  }

  test("RecordStream") {
    val conf = new Configuration
    implicit val rs = SeekableRecordStream(File("5k.bam"))

    checkFirstRecords
  }

  test("SeekableRecordStream") {
    val conf = new Configuration
    implicit val rs = SeekableRecordStream(File("5k.bam"))

    checkFirstRecords

    rs.seek(Pos(2454, 64473))
    rs.curPos should be(Some(Pos(2454, 64473)))

    check(
      ( 2454 → 64473, 12656, "HWI-ST807:461:C2P0JACXX:4:2103:9923:17201"),
      (27784 →     0, 12676, "HWI-ST807:461:C2P0JACXX:4:2310:1258:47908"),
      (27784 →   664, 12682, "HWI-ST807:461:C2P0JACXX:4:2302:19124:35915")
    )

    rs.seek(Pos(0, 0))

    check(
      ( 2454 →     0, 10001, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724"),
      ( 2454 →   624, 10009, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724")
    )

    rs.curPos should be(Some(Pos(2454, 1244)))
    rs.seek(Pos(2454, 1244))
    rs.curPos should be(Some(Pos(2454, 1244)))
    check(
      ( 2454 →  1244, 10048, "HWI-ST807:461:C2P0JACXX:4:1304:9505:89866")
    )
  }
}
