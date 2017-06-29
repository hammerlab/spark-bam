package org.hammerlab.bam

import org.hammerlab.parallel
import org.hammerlab.parallel.spark.Config
import org.hammerlab.parallel.{ spark, threads }
import org.hammerlab.spark.test.suite.SparkSuite

package object spark {

  trait Threads {
    val parallelConfig: parallel.Config = threads.Config(4)
  }

  trait Spark {
    self: SparkSuite ⇒
    import LoadBam.defaultPartitioningStrategy
    lazy val parallelConfig: parallel.Config = Config()
  }

  class LoadBAMThreads
    extends LoadBAMTest
      with Threads

  class LoadBAMSpark
    extends LoadBAMTest
      with Spark

  class LoadSAMThreads
    extends LoadSAMTest
      with Threads

  class LoadSAMSpark
    extends LoadSAMTest
      with Spark
}
