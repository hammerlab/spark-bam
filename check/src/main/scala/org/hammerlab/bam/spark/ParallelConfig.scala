package org.hammerlab.bam.spark

import java.lang.Runtime.getRuntime

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
        parallel.spark.apply(sc, ps)
      case Threads(numThreads) ⇒
        parallel.threads.Config(numThreads)
    }
}

case class Threads(numThreads: Int = getRuntime.availableProcessors())
  extends ParallelConfig

case class Spark(partitioningStrategy: PartitioningStrategy = ElemsPerPartition(1))
  extends ParallelConfig

object Spark {
  implicit def makeSpark(partitioningStrategy: PartitioningStrategy): Spark =
    Spark(partitioningStrategy)
}
