package org.hammerlab.args

import java.lang.{ Long ⇒ JLong }

import caseapp.core.ArgParser
import caseapp.core.ArgParser.instance
import cats.Monoid
import org.hammerlab.bytes.Bytes
import org.hammerlab.guava.collect.{ RangeSet, TreeRangeSet }
import org.hammerlab.guava.collect.Range.closedOpen

sealed trait Range[T] {
  def start: T
  def end: T
}

object Range {
  def unapply[T](range: Range[T]): Option[(T, T)] =
    Some(
      (
        range.start,
        range.end
      )
    )
}

case class Endpoints[T](start: T, end: T) extends Range[T]

case class OffsetLength[T: Monoid](start: T, length: T) extends Range[T] {
  import cats.implicits._
  override def end: T = start |+| length
}

import spire.math.Integral

case class Point[T: Integral](start: T) extends Range[T] {
  import spire.implicits._
  override def end: T = start + 1
}

case class ByteRanges(ranges: Seq[Range[Long]]) {
  @transient lazy val rangeSet: RangeSet[JLong] = {
    val rangeSet = TreeRangeSet.create[JLong]
    ranges.foreach {
      case Range(start, end) ⇒
        rangeSet.add(
          closedOpen[JLong](
            start,
            end
          )
        )
    }
    rangeSet
  }
}

object ByteRanges {
  implicit def unwrapByteRanges(byteRanges: ByteRanges): RangeSet[JLong] = byteRanges.rangeSet

  val rangeRegex = """(.*)-(.*)""".r
  val chunkRegex = """(.+)\+(.+)""".r
  val pointRegex = """(.+)""".r

  implicit val parser: ArgParser[ByteRanges] =
    instance[ByteRanges]("byte ranges") {
      str ⇒
        Right(
          ByteRanges(
            str
              .split(",")
              .map {
                case rangeRegex(from, until) ⇒
                  Endpoints(
                    if (from.isEmpty)
                      0L
                    else
                      Bytes(from).bytes,
                    Bytes(until).bytes
                  )
                case chunkRegex(fromStr, length) ⇒
                  val from = Bytes(fromStr).bytes
                  import cats.implicits.catsKernelStdGroupForLong
                  OffsetLength(
                    from,
                    Bytes(length).bytes
                  )
                case pointRegex(pointStr) ⇒
                  Point(pointStr.toLong)
                case rangeStr ⇒
                  throw new IllegalArgumentException(
                    s"Invalid byte-range $rangeStr in argument $str"
                  )
              }
          )
        )
    }
}
