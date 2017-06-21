package org.hammerlab.bam.spark

import org.hammerlab.parallel
import org.hammerlab.parallel.{ spark, threads }
import org.hammerlab.spark.test.suite.SparkSuite

trait Threads {
  val parallelConfig: parallel.Config = threads.Config(4)
}

trait Spark {
  self: SparkSuite ⇒
  lazy val parallelConfig: parallel.Config = spark.Config()
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
