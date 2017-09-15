package org.hammerlab.bam.iterator

import htsjdk.samtools.SAMRecord
import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.bgzf.Pos
import org.hammerlab.channel.SeekableByteChannel.ChannelByteChannel
import org.hammerlab.test.Suite

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
    rs.header.endPos should be(Pos(0, 5650))
    check(
      (    0 →  5650, 10001, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724"),
      (    0 →  6274, 10009, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724"),
      (    0 →  6894, 10048, "HWI-ST807:461:C2P0JACXX:4:1304:9505:89866"),
      (    0 →  7533, 10335, "HWI-ST807:461:C2P0JACXX:4:2311:6431:65669"),
      (    0 →  8170, 10363, "HWI-ST807:461:C2P0JACXX:4:1305:2342:51860"),
      (    0 →  8738, 10363, "HWI-ST807:461:C2P0JACXX:4:1305:2342:51860"),
      (    0 →  9384, 10368, "HWI-ST807:461:C2P0JACXX:4:1304:9505:89866"),
      (    0 → 10018, 10458, "HWI-ST807:461:C2P0JACXX:4:2311:6431:65669"),
      (    0 → 10637, 11648, "HWI-ST807:461:C2P0JACXX:4:1107:13461:64844"),
      (    0 → 11318, 11687, "HWI-ST807:461:C2P0JACXX:4:2203:17157:59976"),
      Drop(20),
      (    0 → 24334, 11911, "HWI-ST807:461:C2P0JACXX:4:2302:17669:14421"),
      Drop(20),
      (    0 → 37812, 12123, "HWI-ST807:461:C2P0JACXX:4:2101:17257:40389"),
      Drop(20),
      (    0 → 50888, 12298, "HWI-ST807:461:C2P0JACXX:4:1312:6062:40537"),
      Drop(20),
      (    0 → 63908, 12572, "HWI-ST807:461:C2P0JACXX:4:1205:8857:43215"),
      (    0 → 64531, 12575, "HWI-ST807:461:C2P0JACXX:4:2203:7446:41375"),
      (    0 → 65150, 12602, "HWI-ST807:461:C2P0JACXX:4:1211:20695:60465"),
      (26169 →   279, 12604, "HWI-ST807:461:C2P0JACXX:4:1313:17039:71392"),
      (26169 →   901, 12605, "HWI-ST807:461:C2P0JACXX:4:2311:16471:84756")
    )
  }

  val path = bam2

  test("RecordStream") {
    implicit val rs = RecordStream(path.inputStream)

    checkFirstRecords
  }

  test("SeekableRecordStream") {
    implicit val rs = SeekableRecordStream(path)

    checkFirstRecords

    rs.seek(Pos(0, 65150))
    rs.curPos should be(Some(Pos(0, 65150)))

    check(
      (    0 → 65150, 12602, "HWI-ST807:461:C2P0JACXX:4:1211:20695:60465"),
      (26169 →   279, 12604, "HWI-ST807:461:C2P0JACXX:4:1313:17039:71392"),
      (26169 →   901, 12605, "HWI-ST807:461:C2P0JACXX:4:2311:16471:84756")
    )

    rs.seek(Pos(0, 0))

    check(
      (0 → 5650, 10001, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724"),
      (0 → 6274, 10009, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724")
    )

    rs.curPos should be(Some(Pos(0, 6894)))
    rs.seek(Pos(0, 6894))
    rs.curPos should be(Some(Pos(0, 6894)))
    check(
      ( 0 → 6894, 10048, "HWI-ST807:461:C2P0JACXX:4:1304:9505:89866")
    )

    val compressedByteChannel = rs.uncompressedBytes.blockStream.compressedBytes

    compressedByteChannel match {
      case ChannelByteChannel(ch) ⇒
        ch.isOpen should be(true)
      case _ ⇒
        fail("Expected ChannelByteChannel")
    }

    rs.close()

    compressedByteChannel match {
      case ChannelByteChannel(ch) ⇒
        ch.isOpen should be(false)
      case _ ⇒
        fail("Expected ChannelByteChannel")
    }
  }
}
