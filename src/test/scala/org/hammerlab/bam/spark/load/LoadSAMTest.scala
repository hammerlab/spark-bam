package org.hammerlab.bam.spark.load

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.spark._
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.hadoop.splits.MaxSplitSize

class LoadSAMTest
  extends LoadBAMChecks {

  override val file = "5k.sam"

  override def load(maxSplitSize: MaxSplitSize): RDD[SAMRecord] =
    sc
      .loadSam(
        path,
        maxSplitSize
      )

  test("1e6") {
    check(
      MaxSplitSize(1000000),
      1226, 1221, 1244, 1219
    )
  }

  test("1e5") {
    check(
      MaxSplitSize(100000),
      133, 137, 136, 132, 134, 140, 141, 137, 136, 137,
      140, 140, 137, 130, 130, 131, 137, 139, 136, 135,
      139, 139, 137, 137, 140, 141, 140, 141, 139, 140,
      140, 130, 133, 133, 131, 132
    )
  }

  test("indexed disjoint regions") {
    val intervals: LociSet = "1:13000-14000,1:60000-61000"

    {
      val records = sc.loadBamIntervals(path, intervals)

      records.getNumPartitions should be(1)
      records.count should be(129)
    }

    {
      val records =
        sc.loadBamIntervals(
          path,
          intervals,
          splitSize = MaxSplitSize(100000)
        )

      records.getNumPartitions should be(36)
      records.count should be(129)
    }
  }
}
