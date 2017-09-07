package org.hammerlab.bam.spark

import org.apache.spark.SparkContext
import org.hammerlab.parallel
import org.hammerlab.parallel.spark.{ ElemsPerPartition, PartitioningStrategy }

/**
 * Wrapper around [[org.hammerlab.parallel.Config]] machinery that is easier to construct before having access to a
 * [[SparkContext]]; exposed in [[org.hammerlab.bam.spark.load.CanLoadBam]] APIs.
 */
sealed trait ParallelConfig

object ParallelConfig {
  implicit def materialize(config: ParallelConfig)(implicit sc: SparkContext): parallel.Config =
    config match {
      case Spark(ps) ⇒
        parallel.Spark(sc, ps)
      case Threads(numThreads) ⇒
        parallel.Threads(numThreads)
    }
}

case class Threads(numThreads: Int = parallel.defaultNumThreads)
  extends ParallelConfig

case class Spark(partitioningStrategy: PartitioningStrategy = ElemsPerPartition(1))
  extends ParallelConfig

object Spark {
  implicit def makeSpark(partitioningStrategy: PartitioningStrategy): Spark =
    Spark(partitioningStrategy)
}
