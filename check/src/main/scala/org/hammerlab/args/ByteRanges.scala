package org.hammerlab.args

import java.lang.{ Long ⇒ JLong }

import caseapp.core.ArgParser
import org.hammerlab.args.ByteRanges.bytesToJLong
import org.hammerlab.bytes.Bytes

case class ByteRanges(ranges: Seq[Range[Bytes]])
  extends Ranges[Bytes, JLong] {
  override implicit def integral: Integral[JLong] = Integral.long
  override implicit def toSetT: (Bytes) ⇒ JLong = bytesToJLong
}

object ByteRanges {
  implicit def bytesToJLong(bytes: Bytes): JLong = bytes.bytes

  implicit val parser: ArgParser[ByteRanges] =
    Ranges.parser[ByteRanges, Bytes, JLong]
}

case class LongRanges(ranges: Seq[Range[Long]])
  extends Ranges[Long, JLong] {
  override implicit def integral: Integral[JLong] = Integral.long
  override implicit def toSetT: (Long) ⇒ JLong = long2Long
}

case class IntRanges(ranges: Seq[Range[Int]])
  extends Ranges[Int, Integer] {
  override implicit def integral: Integral[Integer] = Integral.integer
  override implicit def toSetT: (Int) ⇒ Integer = int2Integer
}

object IntRanges {
  implicit val parser: ArgParser[IntRanges] =
    Ranges.parser[IntRanges, Int, Integer]
}
