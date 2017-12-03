package org.hammerlab.bam.check.full.error

import hammerlab.print._
import hammerlab.show._
import shapeless.labelled.FieldType

case class Counts(tooFewFixedBlockBytes       : Long,
                  negativeReadIdx             : Long,
                  tooLargeReadIdx             : Long,
                  negativeReadPos             : Long,
                  tooLargeReadPos             : Long,
                  negativeNextReadIdx         : Long,
                  tooLargeNextReadIdx         : Long,
                  negativeNextReadPos         : Long,
                  tooLargeNextReadPos         : Long,
                  tooFewBytesForReadName      : Long,
                  nonNullTerminatedReadName   : Long,
                  nonASCIIReadName            : Long,
                  noReadName                  : Long,
                  emptyReadName               : Long,
                  tooFewBytesForCigarOps      : Long,
                  invalidCigarOp              : Long,
                  emptyMappedCigar            : Long,
                  emptyMappedSeq              : Long,
                  tooFewRemainingBytesImplied : Long,
                  readsBeforeError            : Map[Int, Long]
                 )
  extends Error[Long] {
  def lines(wrapFields: Boolean = false,
            includeZeros: Boolean = true,
            hideTooFewFixedBlockBytes: Boolean = false)(
      implicit indent: Indent
  ): Lines =
    Counts.makeLines(
      wrapFields,
      includeZeros,
      hideTooFewFixedBlockBytes
    ).apply(
      this
    )
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
  implicit def makeLines(wrapFields: Boolean = false,
                         includeZeros: Boolean = true,
                         hideTooFewFixedBlockBytes: Boolean = false)(
      implicit _indent: Indent
  ): ToLines[Counts] =
    new Print(_: Counts) {
      val dc = t.descCounts

      val stringPairs =
        (
          for {
            (k, v) ← dc
            if (v > 0 || includeZeros) && (k != "tooFewFixedBlockBytes" || !hideTooFewFixedBlockBytes)
          } yield
            k → v.toString
        ) ++
        (
          t
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

      if (wrapFields)
        write("Errors(")

      ind {
        stringPairs foreach {
          case (k, v) ⇒
            write(
              s"${" " * (maxKeySize - k.length)}$k:\t${" " * (maxValSize - v.toString.length)}$v"
            )
        }
      }

      if (wrapFields)
        write(")")
  }
}
