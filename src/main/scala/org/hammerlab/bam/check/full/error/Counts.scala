package org.hammerlab.bam.check.full.error

import cats.Show
import cats.Show.show
import shapeless.labelled.FieldType

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
                  emptyMappedCigar: Long,
                  emptyMappedSeq: Long,
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

  object longFields extends FieldPoly {
    implicit def longCase[K](implicit witness: Witness.Aux[K]): Case.Aux[FieldType[K, Long], FieldType[K, Long]] =
      atField(witness)(x ⇒ x)
  }

  implicit class CountsWrapper(val counts: Counts)
    extends AnyVal {

    /**
     * Convert an [[Counts]] to a values-descending array of (key,value) pairs
     */
    def descCounts: List[(String, Long)] =
      lg
        .to(counts)
        .collect(longFields)
        .fields
        .toList[(Symbol, Long)]
        .map {
          case (k, v) ⇒
            k.name → v
        }
        .sortBy(-_._2)
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

        val stringPairs =
          (
            for {
              (k, v) ← dc
              if (v > 0 || includeZeros) && (k != "tooFewFixedBlockBytes" || !hideTooFewFixedBlockBytes)
            } yield
              k → v.toString
          ) ++
          (
            counts
              .readsBeforeError
              .toVector
              .sorted match {
                case Vector() ⇒ Nil
                case readsBeforeError ⇒
                  List(
                    "readsBeforeError" →
                      readsBeforeError
                        .map {
                          case (reads, num) ⇒
                            s"${reads}ⅹ$num"
                        }
                        .mkString(" ")
                  )
              }
          )

        val maxKeySize = stringPairs.map(_._1.length).max
        val maxValSize = stringPairs.map(_._2.length).max

        val lines =
          stringPairs.map {
            case (k, v) ⇒
              s"\t${" " * (maxKeySize - k.length)}$k:\t${" " * (maxValSize - v.toString.length)}$v"
          }

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
