package org.hammerlab.bam.check.full.error

import shapeless.{ Generic, LabelledGeneric }

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
                  tooFewRemainingBytesImplied: Long)
  extends Error[Long]

object Counts {

  /** Can't instantiate these inside the [[CountsWrapper]] [[AnyVal]] below. */
  private val labelledCounts = LabelledGeneric[Counts]
  private val genericCounts = Generic[Counts]

  import shapeless.record._

  implicit class CountsWrapper(val counts: Counts)
    extends AnyVal {

      /**
     * Convert an [[Counts]] to a values-descending array of (key,value) pairs
     */
    def descCounts: Array[(String, Long)] =
      labelledCounts
        .to(counts)
        .toMap
        .toArray
        .map {
          case (k, v) ⇒
            k.name → v
        }
        .sortBy(-_._2)

    /**
     * Pretty-print an [[Counts]]
     */
    def pp(indent: String = "",
           wrapFields: Boolean = false,
           includeZeros: Boolean = true): String = {

      val dc = descCounts

      val maxKeySize = dc.map(_._1.length).max
      val maxValSize = dc.map(_._2.toString.length).max

      val lines =
        for {
          (k, v) ← dc
          if (v > 0 || includeZeros)
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

    /**
     * Count the number of non-zero fields in an [[Counts]]
     */
    def numNonZeroFields: Int =
      genericCounts
        .to(counts)
        .toList[Long]
        .count(_ > 0)
  }


}
