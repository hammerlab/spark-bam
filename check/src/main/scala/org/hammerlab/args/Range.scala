package org.hammerlab.args

import caseapp.core.ArgParser
import caseapp.core.ArgParser.instance
import cats.Monoid
import cats.syntax.all._
import org.hammerlab.args.Range.parserFromStringPair
import org.hammerlab.guava.collect.Range.closedOpen
import org.hammerlab.guava.{ collect ⇒ guava }

sealed trait Range[T]

object Range {

  implicit class IntegralOps[T](t: T)(implicit ev: Integral[T]) {
    def +(o: T): T = ev.+(t, o)
  }

  implicit def toGuavaRange[T, U <: Comparable[U]](range: Range[T])(
      implicit
      integral: Integral[U],
      convert: T ⇒ U
  ): guava.Range[U] =
    range match {
      case Endpoints(start, end) ⇒
        closedOpen[U](start, end)
      case OffsetLength(start, length) ⇒
        closedOpen[U](start, convert(start) + convert(length))
      case Point(start) ⇒
        closedOpen[U](start, convert(start) + integral.one)
    }

  implicit def parser[T, U](implicit
                            elemParser: ArgParser[T],
                            monoid: Monoid[T] = null,
                            integral: Integral[T] = null): ArgParser[Range[T]] =
    instance("endpoint-range") {
      str ⇒
        implicitly[ArgParser[Endpoints[T]]]
          .apply(None, str, mandatory = false)
          .map(_._2)
          .orElse(
            implicitly[ArgParser[OffsetLength[T]]]
              .apply(None, str, mandatory = false)
              .map(_._2)
              .orElse(
                implicitly[ArgParser[Point[T]]]
                  .apply(None, str, mandatory = false)
                  .map(_._2)
              )
          )
    }

  def parserFromStringPair[T](str1: String,
                              str2: String)(
      implicit
      elemParser: ArgParser[T]
  ): Either[String, (T, T)] =
    (
      elemParser(
        None,
        str1,
        mandatory = false
      ),
      elemParser(
        None,
        str2,
        mandatory = false
      )
    ) match {
      case (
        Right((_, from)),
        Right((_, until))
        ) ⇒
        Right(
          (
            from,
            until
          )
        )
      case (Left(fromErr), _) ⇒ Left(fromErr)
      case (_, Left(untilErr)) ⇒ Left(untilErr)
    }
}

case class Endpoints[T](start: T, end: T) extends Range[T]

object Endpoints {
  val regex = """(.*)-(.*)""".r

  implicit def parser[T](implicit elemParser: ArgParser[T]): ArgParser[Endpoints[T]] =
    instance("endpoint-range") {
      case regex(fromStr, untilStr) ⇒
        parserFromStringPair[T](fromStr, untilStr)
          .map {
            case (from, until) ⇒
              Endpoints(from, until)
          }
      case str ⇒
        Left(s"Not a valid start-end range: $str")
    }
}

case class OffsetLength[T: Monoid](start: T,
                                   length: T)
  extends Range[T]

object OffsetLength {
  val regex = """(.+)\+(.+)""".r

  implicit def parser[T: Monoid](implicit elemParser: ArgParser[T]): ArgParser[OffsetLength[T]] =
    instance("endpoint-range") {
      case regex(fromStr, untilStr) ⇒
        parserFromStringPair[T](fromStr, untilStr)
          .map {
            case (from, until) ⇒
              OffsetLength(from, until)
          }
      case str ⇒
        Left(s"Not a valid start+length range: $str")
    }
}

case class Point[T: Integral](start: T)
  extends Range[T]

object Point {
  implicit def parser[T](implicit
                         elemParser: ArgParser[T],
                         integral: Integral[T] = null): ArgParser[Point[T]] =
    instance("point-range") {
      str ⇒
        elemParser
          .apply(None, str, mandatory = false)
          .map(_._2)
          .map(Point(_))
    }
}
