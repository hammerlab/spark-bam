package org.hammerlab.bam.spark.compare

import org.apache.spark.SparkContext
import org.hammerlab.bam.check.{ MaxReadSize, ReadsToCheck }
import org.hammerlab.bgzf.block.BGZFBlocksToCheck
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.paths.Path

/**
 * Wrapper for the split-computation Spark job that uses a [[Configuration]] in each task; forces the closure's
 * reference to the [[Configuration]] to be an instance variable that gets serialized, as opposed to [[Main.conf]] which
 * is static and would inadvertently cause creation of a [[SparkContext]] on each executor.
 */
class PathChecks(lines: Vector[String], num: Int)(
    implicit
    sc: SparkContext,
    conf: Configuration,
    splitSize: MaxSplitSize,
    bgzfBlocksToCheck: BGZFBlocksToCheck,
    readsToCheck: ReadsToCheck,
    maxReadSize: MaxReadSize
)
  extends Serializable {
  val results =
    sc
      .parallelize(
        lines,
        numSlices = num
      )
      .map {
        bamPathStr ⇒
          val bamPath = Path(bamPathStr)

          bamPath →
            Result(bamPath)
      }
      .cache
}
