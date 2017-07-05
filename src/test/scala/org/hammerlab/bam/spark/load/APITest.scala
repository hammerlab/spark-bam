package org.hammerlab.bam.spark.load

import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File

class APITest
  extends SparkSuite {

  test("sample load calls") {

    import org.hammerlab.bam.spark._

    val path = File("5k.bam").path

    sc.loadBam(path)

    import org.hammerlab.bytes._
    sc.loadBam(path, Threads(32), splitSize = 32.MB)
    sc.loadBam(path, Threads(32), splitSize = 1 << 25)

    import org.hammerlab.parallel.spark._
    sc.loadBam(path, Spark())
    sc.loadBam(path, Spark(ElemsPerPartition(10)))
    sc.loadBam(path, Spark(NumPartitions(2)))
  }

}
