package org.hammerlab.bgzf.block

import java.nio.channels.FileChannel

import org.hammerlab.bgzf.Pos
import org.hammerlab.io.ByteChannel
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class ByteStreamTest
  extends Suite {

  def checkHeader(implicit byteStream: UncompressedBytesI[_], byteChannel: ByteChannel): Unit = {
    byteChannel.readString(4, includesNull = false) should be("BAM\1")

    val headerTextLength = 4253
    byteChannel.getInt should be(headerTextLength)

    val headerStr = byteChannel.readString(headerTextLength)

    headerStr.take(100) should be(
      """@HD	VN:1.4	GO:none	SO:coordinate
        |@SQ	SN:1	LN:249250621
        |@SQ	SN:2	LN:243199373
        |@SQ	SN:3	LN:198022430
        |@"""
      .stripMargin
    )

    val numReferenceSequences = byteChannel.getInt
    numReferenceSequences should be(84)

    // skip to last reference-sequence-length
    byteChannel.skip(5646 - byteChannel.position().toInt)

    byteStream.curPos should be(Some(Pos(0, 5646)))

    byteChannel.getInt should be(547496)

    byteStream.curPos should be(Some(Pos(2454, 0)))

    byteChannel.getInt should be(620)
    byteStream.curPos should be(Some(Pos(2454, 4)))

    // Skip to 4 bytes from the end of this block
    byteChannel.skip(5650 + 65092 - 4 - byteChannel.position().toInt)
    byteStream.clear()
    byteStream.curPos should be(Some(Pos(2454, 65092 - 4)))
  }

  test("ByteStream") {
    implicit val byteStream =
      UncompressedBytes(
        File("5k.bam")
          .inputStream
      )

    implicit val byteChannel: ByteChannel = byteStream

    checkHeader
  }

  test("SeekableByteStream") {
    implicit val byteStream =
      SeekableUncompressedBytes(
        FileChannel.open(File("5k.bam").path)
      )

    implicit val byteChannel: ByteChannel = byteStream

    checkHeader

    def checkRead(): Unit = {
      byteStream.seek(Pos(27784, 11033))
      byteChannel.getInt should be(642)
      byteChannel.getInt should be(0)
      byteChannel.getInt should be(12815)
      val readNameLen = byteChannel.getInt & 0xff
      byteChannel.skip(20)
      byteChannel.readString(readNameLen) should be("HWI-ST807:461:C2P0JACXX:4:2311:16471:84756")
    }

    checkRead()
    checkRead()

    byteStream.seek(Pos(0, 0))
    val freshByteChannel: ByteChannel = byteStream
    checkHeader(byteStream, freshByteChannel)
  }
}
