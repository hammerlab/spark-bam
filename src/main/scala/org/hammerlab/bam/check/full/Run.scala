package org.hammerlab.bam.check.full

import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.full.error.Flags.toCounts
import org.hammerlab.bam.check.full.error.{ Counts, Flags }
import org.hammerlab.bam.check.{ Args, False, Negative, Positive, True, UncompressedStreamRun }
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableUncompressedBytes
import org.hammerlab.magic.rdd.partitions.OrderedRepartitionRDD._
import org.hammerlab.magic.rdd.size._
import org.hammerlab.math.Monoid.zero
import org.hammerlab.math.MonoidSyntax._
import org.hammerlab.math.ceil
import org.hammerlab.paths.Path
import org.hammerlab.spark.{ Context, Partitioner }

import scala.collection.SortedMap
import scala.math.min

/**
 * [[check.Run]] implementation that uses the "full" [[Checker]], which records information about as many checks' as
 * possible at each [[Pos]], for analyzing which rules are useful/necessary to correctly identify
 * read-record-boundaries.
 */
object Run
  extends check.Run[Option[Flags], PosResult]
    with UncompressedStreamRun[Option[Flags], PosResult] {

  override def makeChecker: (SeekableUncompressedBytes, ContigLengths) ⇒ Checker =
    Checker.apply

  override def makePosResult: check.MakePosResult[Option[Flags], PosResult] = MakePosResult

  override def apply(args: Args)(implicit sc: Context, path: Path): Result = {
    val (calls, filteredBlockSet) = getCalls(args)

    val trueReadPositions = getTrueReadPositions(args, filteredBlockSet)

    /**
     * Join per-[[Pos]] [[Call]]s against the set of true read-record boundaries, [[trueReadPositions]], making a
     * [[PosResult]] for each that records {[[True]],[[False]]} x {[[Positive]],[[Negative]]} information as well
     * as optional info from the [[Call]].
     */
    val results: RDD[(Pos, PosResult)] =
      calls
        .fullOuterJoin(
          trueReadPositions.map(
            _ → null
          )
        )
        .map {
          case (pos, (callOpt, isReadStart)) ⇒
            pos →
              callOpt
              .map {
                call ⇒
                  makePosResult(
                    call,
                    isReadStart.isDefined
                  )
              }
              .getOrElse(
                throw new Exception(s"No call detected at $pos")
              )
        }
        .sortByKey()

    val posResultPartitioner =
      Partitioner[check.PosResult](
        4,
        {
          case _: check.TruePositive ⇒ 0
          case _: check.TrueNegative ⇒ 1
          case _: check.FalsePositive ⇒ 2
          case _: check.FalseNegative ⇒ 3
        }
      )

    /** Compute {[[True]],[[False]]} x {[[Positive]],[[Negative]]} counts in one stage */
    val trueFalseCounts: Map[check.PosResult, Long] =
      results
        .values
        .map {
          case _: check. TruePositive ⇒ check. TruePositive → 1L
          case _: check. TrueNegative ⇒ check. TrueNegative → 1L
          case _: check.FalsePositive ⇒ check.FalsePositive → 1L
          case _: check.FalseNegative ⇒ check.FalseNegative → 1L
        }
        .reduceByKey(
          posResultPartitioner,
          _ + _
        )
        .collectAsMap
        .toMap

    val numCalls = trueFalseCounts.values.sum

    val numFalseCalls =
      trueFalseCounts
        .filterKeys {
          case _: False ⇒ true
          case _: True ⇒ false
        }
        .values
        .sum

    val falseCalls =
      results
        .flatMap {
          case (pos, f: False) ⇒
            Some(pos → (f: False))
          case _ ⇒
            None
        }
        .orderedRepartition(
          min(
            results.getNumPartitions,
            ceil(
              numFalseCalls,
              args.resultsPerPartition
            )
            .toInt
          )
        )

    implicit val sampleSize = args.printLimit

    /**
     * How many times each flag correctly rules out a [[Pos]], grouped by how many total flags ruled out that [[Pos]].
     *
     * Useful for identifying e.g. flags that tend to be "critical" (necessary to avoid [[PosResult]] read-boundary
     * [[Call]]s).
     */
    val trueNegativesByNumNonzeroFields: Array[(Int, Counts)] =
      results
        .values
        .flatMap {
          case TrueNegative(error) ⇒ Some(toCounts(error))
          case _ ⇒ None
        }
        .keyBy(_.numNonZeroFields)
        .reduceByKey(_ |+| _, 20)  // Total number of distinct keys will be the number of fields in an [[ErrorT]]
        .collect()
        .sortBy(_._1)

    /**
     * CDF to [[trueNegativesByNumNonzeroFields]]'s PDF: how many times does each flag correctly rule out [[Pos]]s that
     * were ruled out by *at most `n`* total flags, for each `n`.
     */
    val countsByNonZeroFields: Array[(Int, (Counts, Counts))] =
      trueNegativesByNumNonzeroFields
        .scanLeft(
          0 → (zero[Counts], zero[Counts])
        ) {
          case (
            (_, (_, countSoFar)),
            (numNonZeroFields, count)
            ) ⇒
            numNonZeroFields →
              (
                count,
                countSoFar |+| count
              )
        }
        .drop(1)  // Discard the dummy/initial "0" entry added above to conform to [[scanLeft]] API

    /**
     * "Critical" error counts: how many times each flag was the *only* flag identifying a read-boundary-candidate as
     * false.
     */
    val criticalErrorCounts = countsByNonZeroFields.head._2._1

    /**
     * "Total" error counts: how many times each flag ruled out a position, over the entire dataset
     */
    val totalErrorCounts = countsByNonZeroFields.last._2._2

    Result(
      numCalls,
      numFalseCalls,
      falseCalls,
      trueReadPositions.size,
      criticalErrorCounts,
      totalErrorCounts,
      SortedMap(countsByNonZeroFields: _*)
    )
  }
}

