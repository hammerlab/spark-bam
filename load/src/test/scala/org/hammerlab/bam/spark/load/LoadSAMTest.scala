package org.hammerlab.bam.spark.load

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.hadoop.splits.MaxSplitSize
import spark_bam._

class LoadSAMTest
  extends LoadBAMChecks {

  val path = sam2

  override def load(maxSplitSize: MaxSplitSize): RDD[SAMRecord] =
    sc
      .loadSam(
        path,
        maxSplitSize
      )

  test("1e6") {
    check(
      MaxSplitSize(1000000),
      1255, 1245
    )
  }

  test("1e5") {
    check(
      MaxSplitSize(100000),
      129, 133, 131, 129, 130, 134, 137, 134, 130, 134,
      135, 135, 136, 135, 125, 130, 128, 127, 128
    )
  }

  test("indexed disjoint regions") {
    val intervals: LociSet = "1:13000-14000,1:28000-29000"

    {
      val records = 
        sc.loadBamIntervals(
          path, 
          intervals
        )

      records.getNumPartitions should be(1)
      records.count should be(125)
    }

    {
      val records =
        sc.loadBamIntervals(
          path,
          intervals,
          splitSize = MaxSplitSize(100000)
        )

      records.getNumPartitions should be(19)
      records.count should be(125)
    }
  }
}
