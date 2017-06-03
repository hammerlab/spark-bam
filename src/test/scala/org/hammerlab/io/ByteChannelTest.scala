package org.hammerlab.io

import java.io.{ EOFException, IOException, InputStream }

import org.hammerlab.test.Suite

case class InputStreamStub(reads: String*)
  extends InputStream {
  val bytes =
    reads
      .flatMap(
        _
          .map(_.toInt)
          .toVector :+ -1
      )
      .iterator

  override def read(): Int =
    if (bytes.hasNext)
      bytes.next()
    else
      throw new EOFException()
}

class ByteChannelTest
  extends Suite {
  test("incomplete InputStream read") {
    val ch: ByteChannel =
      InputStreamStub(
        "12345",
        "67890",
        "1",
        "",
        "234"
      )

    val b4 = Buffer(4)

    ch.read(b4)
    b4.array.map(_.toChar).mkString("") should be("1234")

    ch.read(b4)
    b4.array.map(_.toChar).mkString("") should be("5678")

    intercept[IOException] {
      ch.read(b4) should be(4)
    }.getMessage should be("Only read 3 of 4 bytes from position 8")
  }
}
