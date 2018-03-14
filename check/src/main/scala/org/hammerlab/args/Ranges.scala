package org.hammerlab.args

import caseapp.core.argparser._
import caseapp.core.Error
import caseapp.core.Error.UnrecognizedValue
import cats.syntax.either._
import org.hammerlab.args.Range.toGuavaRange
import org.hammerlab.guava.collect.{ RangeSet, TreeRangeSet }
import shapeless._

trait Integral[T] {
  def +(a: T, b: T): T
  def one: T
}

import java.lang.{ Long ⇒ JLong }

object Integral {
  implicit val long: Integral[JLong] =
    new Integral[JLong] {
      override def +(a: JLong, b: JLong): JLong = a + b
      override def one: JLong = 1L
    }

  implicit val integer: Integral[Integer] =
    new Integral[Integer] {
      override def +(a: Integer, b: Integer): Integer = a + b
      override def one: Integer = 1
    }
}

trait Ranges[T, SetT <: Comparable[SetT]] {

  def ranges: Seq[Range[T]]
  implicit def integral: Integral[SetT]
  implicit def toSetT: T ⇒ SetT

  /**
   * This is @transient and lazy – and here instead of replacing the ctor param above that generates it – so that
   * [[Ranges]] can be serialized, since [[RangeSet]] is not [[Serializable]].
   */
  @transient lazy val rangeSet: RangeSet[SetT] = {
    val rangeSet = TreeRangeSet.create[SetT]
    ranges.foreach {
      range ⇒
        rangeSet.add(toGuavaRange[T, SetT](range))
    }
    rangeSet
  }
}

object Ranges {
  implicit def unwrapRanges[T, SetT <: Comparable[SetT]](ranges: Ranges[T, SetT]): RangeSet[SetT] = ranges.rangeSet

  val rangeRegex = """(.*)-(.*)""".r
  val chunkRegex = """(.+)\+(.+)""".r
  val pointRegex = """(.+)""".r

  implicit def parser[R <: Ranges[T, SetT], T, SetT <: Comparable[SetT]](
      implicit
      gen: Generic.Aux[R, Seq[Range[T]] :: HNil],
      rangeParser: ArgParser[Range[T]],
      toSetT: T ⇒ SetT
  ): ArgParser[R] =
    SimpleArgParser.from[R]("ranges") {
      _
        .split(",")
        .map {
          rangeParser(None, _)
        }
        .foldLeft[Either[Seq[Error], Seq[Range[T]]]](
          Right(Vector())
        ) {
          case (Left(errors), Left(error)) ⇒ Left(errors :+ error)
          case (_, Left(error)) ⇒ Left(Vector(error))
          case (Left(errors), _) ⇒ Left(errors)
          case (Right(ranges), Right(range)) ⇒ Right(ranges :+ range)
        }
        .bimap(
          errors ⇒ UnrecognizedValue(s"Invalid ranges:\n\t${errors.mkString("\n\t")}"),
          ranges ⇒ gen.from(ranges :: HNil)
        )
    }
}
