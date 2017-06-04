package org.hammerlab.bam.check.full

import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.False
import org.hammerlab.bam.check.full.error.Counts
import org.hammerlab.bgzf.Pos

import scala.collection.SortedMap

/**
 * Statistics about a [[Checker]]'s performance identifying read-record-boundaries in a BAM file.
 *
 * @param numCalls              number of [[Pos]]s evaluated; this is the block-decompressed size of the input BAM file
 * @param results               [[RDD]] of [[PosResult]]s, keyed by [[Pos]]
 * @param numFalseCalls         number of "false [[PosResult]]s": read-boundaries that were ruled out by [[Checker]]
 *                              ([[FalseNegative]]s) and read-boundaries predicted by [[Checker]] that aren't actually
 *                              read-record-boundaries in the input BAM ([[FalsePositive]]s)
 * @param falseCalls            [[RDD]] of [[False]] [[PosResult]]s
 * @param criticalErrorCounts   how many times each flag was the *only* flag identifying a [[TrueNegative]]
 *                              read-boundary-candidate as negative
 * @param totalErrorCounts      how many times each flag identified a [[TrueNegative]] read-boundary-candidate as negative
 * @param countsByNonZeroFields for each `n`:
 *                              - the number of times each flag was set at [[Pos]]s with exactly `n` flags set, as well
 *                                as…
 *                              - the number of times it was set for [[Pos]]s with `≤ n` flags set
 */

case class Result(numCalls: Long,
                  results: RDD[(Pos, PosResult)],
                  numFalseCalls: Long,
                  falseCalls: RDD[(Pos, False)],
                  criticalErrorCounts: Counts,
                  totalErrorCounts: Counts,
                  countsByNonZeroFields: SortedMap[Int, (Counts, Counts)])
  extends check.Result[PosResult]

