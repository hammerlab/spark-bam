package org.hammerlab.args

import caseapp.core.ArgParser
import caseapp.core.ArgParser.instance
import cats.syntax.all._
import org.hammerlab.bytes.Bytes
import org.hammerlab.guava.collect.Range.closedOpen
import org.hammerlab.guava.collect.{ RangeSet, TreeRangeSet }
import shapeless._

abstract class Ranges[T, SetT <: Comparable[SetT]](ranges: Seq[Range[T]])(implicit toSetT: T ⇒ SetT) {
  /**
   * This is @transient and lazy – and here instead of replacing the ctor param above that generates it – so that
   * [[Ranges]] can be serialized, since [[RangeSet]] is not [[Serializable]].
   */
  @transient lazy val rangeSet: RangeSet[SetT] = {
    val rangeSet = TreeRangeSet.create[SetT]
    ranges.foreach {
      case Range(start, end) ⇒
        rangeSet.add(
          closedOpen[SetT](
            start,
            end
          )
        )
    }
    rangeSet
  }
}

object Ranges {
  implicit def unwrapRanges[T, SetT <: Comparable[SetT]](ranges: Ranges[T, SetT]): RangeSet[SetT] = ranges.rangeSet

  import java.lang.{ Long ⇒ JLong }

  type ByteRanges = Ranges[Bytes, JLong]

  val rangeRegex = """(.*)-(.*)""".r
  val chunkRegex = """(.+)\+(.+)""".r
  val pointRegex = """(.+)""".r

  implicit def parser[R <: Ranges[T, SetT], T, SetT <: Comparable[SetT]](
      implicit
      gen: Generic.Aux[R, Seq[Range[T]] :: HNil],
      rangeParser: ArgParser[Range[T]],
      toSetT: T ⇒ SetT
  ): ArgParser[R] =
    instance[R]("ranges") {
      _
        .split(",")
        .map {
          str ⇒
            rangeParser
              .apply(None, str, mandatory = false)
              .map(_._2)
        }
        .foldLeft[Either[Seq[String], Seq[Range[T]]]](
          Right(Vector())
        ) {
          case (Left(errors), Left(error)) ⇒ Left(errors :+ error)
          case (_, Left(error)) ⇒ Left(Vector(error))
          case (Left(errors), _) ⇒ Left(errors)
          case (Right(ranges), Right(range)) ⇒ Right(ranges :+ range)
        }
        .bimap(
          errors ⇒ s"Invalid ranges:\n\t${errors.mkString("\n\t")}",
          ranges ⇒ gen.from(ranges :: HNil)
        )
    }
}
