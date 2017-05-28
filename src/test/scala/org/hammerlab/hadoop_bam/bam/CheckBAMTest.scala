package org.hammerlab.hadoop_bam.bam

import org.apache.hadoop.fs.Path
import org.hammerlab.hadoop_bam.bam.Error.ErrorCount
import org.hammerlab.hadoop_bam.bgzf.Pos
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File
import org.hammerlab.test.matchers.seqs.SeqMatcher.seqMatch
import shapeless._
import Error.{toCounts, numNonZeroFields}
//import spire.algebra.Monoid
import Monoid.{ mzero ⇒ zero }

trait MonoidSyntax[T] {
  def |+|(b: T): T
}

object MonoidSyntax {
  implicit def monoidSyntax[T](a: T)(implicit mt: Monoid[T]): MonoidSyntax[T] = new MonoidSyntax[T] {
    def |+|(b: T) = mt.append(a, b)
  }
}

trait Monoid[T] {
  def zero: T
  def append(a: T, b: T): T
}

object Monoid extends ProductTypeClassCompanion[Monoid] {
  def mzero[T](implicit mt: Monoid[T]) = mt.zero

  implicit def longMonoid: Monoid[Long] = new Monoid[Long] {
    def zero = 0
    def append(a: Long, b: Long) = a+b
  }

  object typeClass extends ProductTypeClass[Monoid] {
    def emptyProduct = new Monoid[HNil] {
      def zero = HNil
      def append(a: HNil, b: HNil) = HNil
    }

    def product[F, T <: HList](mh: Monoid[F], mt: Monoid[T]) = new Monoid[F :: T] {
      def zero = mh.zero :: mt.zero
      def append(a: F :: T, b: F :: T) = mh.append(a.head, b.head) :: mt.append(a.tail, b.tail)
    }

    def project[F, G](instance: => Monoid[G], to: F => G, from: G => F) = new Monoid[F] {
      def zero = from(instance.zero)
      def append(a: F, b: F) = from(instance.append(to(a), to(b)))
    }
  }
}

class CheckBAMTest
  extends SparkSuite {

  import MonoidSyntax._

  test("monoids") {
    val errors =
      Seq(
        Error(true, None, None, None, None, false),
        Error(true, None, None, None, None, true)
      )

    val counts = errors.map(Error.toCounts)

    counts(0) |+| counts(1) should be(
      ErrorT[Long](2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1)
    )
  }

  test("check") {
    val calls =
      CheckBAM.run(
        sc,
        new Path(File("2.bam")),
        CheckBAMArgs(
          File("blocks"),
          File("records"),
          File("2.bam"),
          Some(100)
        )
      )

    calls.count should be(6549800)

    val falseCalls =
      calls.filter {
        _._2 match {
          case f: False ⇒ true
          case _ ⇒ false
        }
      }

    falseCalls.count should be(0)

    val trueNegativesByNumFields: Array[(Int, ErrorCount)] =
      calls
        .flatMap {
          _._2 match {
            case TrueNegative(error) ⇒ Some(toCounts(error))
            case _ ⇒ None
          }
        }
        .map(counts ⇒
          numNonZeroFields(counts) → counts
        )
        .reduceByKey(_ |+| _)
        .collect()
        .sortBy(_._1)

    val trueNegativesByNumFieldsCumulative =
      trueNegativesByNumFields
        .map(_._2)
        .scanLeft(
          zero[ErrorCount]
        )(
          _ |+| _
        )
        .drop(1)

    println(trueNegativesByNumFieldsCumulative.mkString("\n"))

    trueNegativesByNumFieldsCumulative.last should be(
      ErrorT[Long](
        invalidCigarOp            = 6168038,
        tooLargeReadIdx           = 6027725,
        tooLargeNextReadIdx       = 6027725,
        tooFewBytesForSeqAndQuals = 5769355,
        nonNullTerminatedReadName = 5134158,
        nonASCIIReadName          =  685293,
        noReadName                =  641496,
        negativeReadPos           =  333781,
        negativeReadIdx           =  333781,
        negativeNextReadPos       =  333781,
        negativeNextReadIdx       =  333781,
        emptyReadName             =   51823,
        tooLargeReadPos           =   43922,
        tooLargeNextReadPos       =   43922,
        tooFewBytesForCigarOps    =   42542,
        tooFewFixedBlockBytes     =       0
      )
    )
  }

//  test("fail") {
//    val errors =
//      CheckBAM.run(
//        sc,
//        new Path(File("2.bam")),
//        Args(
//          File("blocks"),
//          File("records"),
//          File("2.bam"),
//          blocksWhitelist = Some("268458108")
//        )
//      )
//      .collect
//
//    errors should be(
//      Array(
//        FalsePositive(
//          Pos(268458108, 115),
//          Pos(268458108, 116),
//          true
//        )
//      )
//    )
//  }
//
//  test("fail2") {
//    val errors =
//      CheckBAM.run(
//        sc,
//        new Path(File("2.bam")),
//        Args(
//          File("blocks"),
//          File("records"),
//          File("2.bam"),
//          blocksWhitelist = Some("201206175,215198728,248476903,253384328")
//        )
//      )
//      .collect
//
//    errors should be(
//      Array(
//        FalsePositive(Pos(201206175,  6940), Pos(201206175,  6941), true),
//        FalsePositive(Pos(215198728, 50073), Pos(215198728, 50074), true),
//        FalsePositive(Pos(248476903, 30794), Pos(248476903, 30795), true),
//        FalsePositive(Pos(253384328, 55615), Pos(253384328, 55616), true)
//      )
//    )
//  }

}
