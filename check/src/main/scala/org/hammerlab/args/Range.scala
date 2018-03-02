package org.hammerlab.args

import caseapp.core.Error
import caseapp.core.Error.UnrecognizedValue
import caseapp.core.argparser._
import cats.Monoid
import cats.syntax.either._
import org.hammerlab.args.Range.parserFromStringPair
import org.hammerlab.guava.collect.Range.closedOpen
import org.hammerlab.guava.{ collect ⇒ guava }
import org.hammerlab.kryo.{ AlsoRegister, cls }

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
    SimpleArgParser.from("endpoint-range") {
      str ⇒
        implicitly[ArgParser[Endpoints[T]]]
          .apply(None, str)
          .orElse(
            implicitly[ArgParser[OffsetLength[T]]]
              .apply(None, str)
              .orElse(
                implicitly[ArgParser[Point[T]]]
                  .apply(None, str)
              )
          )
    }

  def parserFromStringPair[T](str1: String,
                              str2: String)(
      implicit
      elemParser: ArgParser[T]
  ): Either[Error, (T, T)] =
    (
      elemParser(
        None,
        str1
      ),
      elemParser(
        None,
        str2
      )
    ) match {
      case (
        Right( from),
        Right(until)
        ) ⇒
        Right(
          (
            from,
            until
          )
        )
      case (Left(fromErr), _ ) ⇒ Left( fromErr)
      case (_, Left(untilErr)) ⇒ Left(untilErr)
    }

  implicit def alsoRegisterRange[_]: AlsoRegister[Range[_]] =
    AlsoRegister(
      cls[Endpoints[_]],
      cls[OffsetLength[_]],
      cls[Point[_]]
    )
}

case class Endpoints[T](start: T, end: T) extends Range[T]

object Endpoints {
  val regex = """(.*)-(.*)""".r

  implicit def parser[T](implicit elemParser: ArgParser[T]): ArgParser[Endpoints[T]] =
    SimpleArgParser.from("endpoint-range") {
      case regex(fromStr, untilStr) ⇒
        parserFromStringPair[T](fromStr, untilStr)
          .map {
            case (from, until) ⇒
              Endpoints(from, until)
          }
      case str ⇒
        Left(UnrecognizedValue(s"Not a valid start-end range: $str"))
    }
}

case class OffsetLength[T: Monoid](start: T,
                                   length: T)
  extends Range[T]

object OffsetLength {
  val regex = """(.+)\+(.+)""".r

  implicit def parser[T: Monoid](implicit elemParser: ArgParser[T]): ArgParser[OffsetLength[T]] =
    SimpleArgParser.from("endpoint-range") {
      case regex(fromStr, untilStr) ⇒
        parserFromStringPair[T](fromStr, untilStr)
          .map {
            case (from, until) ⇒
              OffsetLength(from, until)
          }
      case str ⇒
        Left(UnrecognizedValue(s"Not a valid start+length range: $str"))
    }
}

case class Point[T: Integral](start: T)
  extends Range[T]

object Point {
  implicit def parser[T](implicit
                         elemParser: ArgParser[T],
                         integral: Integral[T] = null): ArgParser[Point[T]] =
    SimpleArgParser.from("point-range") {
      str ⇒
        elemParser
          .apply(None, str)
          .map(Point(_))
    }
}
