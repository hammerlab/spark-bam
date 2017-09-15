package org.hammerlab.bgzf.block

import java.nio.channels.FileChannel

import org.hammerlab.bgzf.Pos
import org.hammerlab.channel.ByteChannel
import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.test.Suite

class ByteStreamTest
  extends Suite {

  def checkHeader(implicit byteStream: UncompressedBytesI[_], byteChannel: ByteChannel): Unit = {
    byteChannel.readString(4, includesNull = false) should be("BAM\1")

    val headerTextLength = 4253
    byteChannel.getInt should be(headerTextLength)

    val headerStr = byteChannel.readString(headerTextLength)

    headerStr.take(100) should be(
      """@HD	VN:1.5	GO:none	SO:coordinate
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

    byteChannel.getInt should be(547496)  // Last reference sequence length

    byteStream.curPos should be(Some(Pos(0, 5650)))

    byteChannel.getInt should be(620)  // First record length, in (uncompressed) bytes
    byteStream.curPos should be(Some(Pos(0, 5654)))

    val firstBlockLength = 65498

    // Skip to 4 bytes from the end of this block
    byteChannel.skip(firstBlockLength - 4 - byteChannel.position().toInt)
    byteStream.clear()
    byteStream.curPos should be(Some(Pos(0, firstBlockLength - 4)))

    byteChannel.getInt
    byteStream.curPos should be(Some(Pos(26169, 0)))
  }

  test("ByteStream") {
    implicit val byteStream =
      UncompressedBytes(
        bam2.inputStream
      )

    implicit val byteChannel: ByteChannel = byteStream

    checkHeader
  }

  test("SeekableByteStream") {
    implicit val byteStream =
      SeekableUncompressedBytes(bam2)

    implicit val byteChannel: ByteChannel = byteStream

    checkHeader

    def checkRead(): Unit = {
      byteStream.seek(Pos(26169, 16277))
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
