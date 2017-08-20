package org.hammerlab.bam.check.full.error

import cats.Show
import cats.Show.show

case class Counts(tooFewFixedBlockBytes: Long,
                  negativeReadIdx: Long,
                  tooLargeReadIdx: Long,
                  negativeReadPos: Long,
                  tooLargeReadPos: Long,
                  negativeNextReadIdx: Long,
                  tooLargeNextReadIdx: Long,
                  negativeNextReadPos: Long,
                  tooLargeNextReadPos: Long,
                  tooFewBytesForReadName: Long,
                  nonNullTerminatedReadName: Long,
                  nonASCIIReadName: Long,
                  noReadName: Long,
                  emptyReadName: Long,
                  tooFewBytesForCigarOps: Long,
                  invalidCigarOp: Long,
                  tooFewRemainingBytesImplied: Long,
                  readsBeforeError: Map[Int, Long]
                 )
  extends Error[Long] {
  def show(indent: String = "",
           wrapFields: Boolean = false,
           includeZeros: Boolean = true,
           hideTooFewFixedBlockBytes: Boolean = false): String =
    Counts.makeShow(
      indent,
      wrapFields,
      includeZeros,
      hideTooFewFixedBlockBytes
    )
    .show(this)
}

object Counts {

  import shapeless._
  import shapeless.record._
  import ops.record._

  /** Can't instantiate these inside the [[CountsWrapper]] [[AnyVal]] below. */
  val lg = LabelledGeneric[Counts]
  val gen = Generic[Counts]

  object longFields extends Poly1 {
    implicit val longCase: Case.Aux[(Symbol, Long), (Symbol, Long)] =
      at(x ⇒ x)
  }

  implicit class CountsWrapper(val counts: Counts)
    extends AnyVal {

      /**
     * Convert an [[Counts]] to a values-descending array of (key,value) pairs
     */
    def descCounts: Array[(String, Long)] =
      /*lg
        .to(counts)
        .collect(longFields)
        .toMap
        .toArray
        .map {
          case (k, v) ⇒
            k.name → v
        }
        .sortBy(-_._2)*/
      ???

    /**
     * Count the number of non-zero fields in an [[Counts]]
     */
    def numNonZeroFields: Int =
      /*gen
        .to(counts)
        .collect(longFields)
        .toList[Long]
        .count(_ > 0)*/
      ???
  }

  /**
   * Pretty-print an [[Counts]]
   */
  implicit def makeShow(indent: String = "",
                        wrapFields: Boolean = false,
                        includeZeros: Boolean = true,
                        hideTooFewFixedBlockBytes: Boolean = false): Show[Counts] =
    show {
      counts ⇒

        val dc = counts.descCounts

        val maxKeySize = dc.map(_._1.length).max
        val maxValSize = dc.map(_._2.toString.length).max

        val lines =
          for {
            (k, v) ← dc
            if (v > 0 || includeZeros) && (k != "tooFewFixedBlockBytes" || !hideTooFewFixedBlockBytes)
          } yield
            s"${" " * (maxKeySize - k.length)}$k:\t${" " * (maxValSize - v.toString.length)}$v"

        if (wrapFields)
          lines
            .mkString(
              s"${indent}Errors(\n\t$indent",
              s"\n\t$indent",
              s"\n$indent)"
            )
        else
          lines
            .mkString(
              indent,
              s"\n$indent",
              ""
            )
    }
}
