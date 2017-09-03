package org.hammerlab.args

import java.lang.{ Long ⇒ JLong }

import caseapp.core.ArgParser
import org.hammerlab.bytes.Bytes

case class ByteRanges(ranges: Seq[Range[Bytes]])
  extends Ranges[Bytes, JLong](ranges)(_.bytes)

object ByteRanges {
  implicit def bytesToJLong(bytes: Bytes): JLong = bytes.bytes

  implicit val parser: ArgParser[ByteRanges] =
    Ranges.parser[ByteRanges, Bytes, JLong]
}

case class LongRanges(ranges: Seq[Range[Long]])
  extends Ranges[Long, JLong](ranges)

case class IntRanges(ranges: Seq[Range[Int]])
  extends Ranges[Int, Integer](ranges)
