package org.hammerlab.args

import cats.Monoid
import cats.syntax.all._
import caseapp.core.ArgParser
import caseapp.core.ArgParser.instance
import Range.parserFromStringPair
import shapeless.Lazy
import spire.math.Integral

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

  implicit def parser[T](implicit
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
  extends Range[T] {
  import cats.implicits._
  override def end: T = start |+| length
}

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

case class Point[T: Integral](start: T) extends Range[T] {
  import spire.implicits._
  override def end: T = start + 1
}

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
