package org.hammerlab.bam.spark.load

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.bam.spark.load.CanLoadBam.getIntevalChunks
import org.hammerlab.bgzf.Pos
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.hadoop.splits.MaxSplitSize
import spark_bam._

class LoadBAMTest
  extends LoadBAMChecks {

  val path = bam2

  override def load(maxSplitSize: MaxSplitSize): RDD[SAMRecord] =
    sc
      .loadReads(
        path,
        splitSize = maxSplitSize
      )

  test("1e6") {
    check(
      MaxSplitSize(1000000),
      2500
    )
  }

  test("1e5") {
    check(
      MaxSplitSize(100000),
      503, 414, 518, 421, 493, 151
    )
  }

  test("2e4") {
    check(
      MaxSplitSize(20000),
       96, 102, 105, 101,  99, 102, 101, 106,   0, 105,
      105, 102, 104, 103, 104, 106, 104, 106,   0, 105,
      195, 101,   0,  99,  98,  99,  52
    )
  }

  test("indexed all") {
    val intervals: LociSet = "1:0-100000"

    getIntevalChunks(
      path,
      intervals
    ) should be(
      Seq(
        Chunk(
          Pos(     0, 5650),
          Pos(531725,    0)
        )
      )
    )

    val records =
      sc.loadBamIntervals(path, intervals)

    records.count should be(2450)  // 2500 reads, 50 unmapped
  }

  test("indexed disjoint regions") {
    val intervals: LociSet = "1:13000-14000,1:60000-61000"

    getIntevalChunks(
      path,
      intervals
    ) should be(
      Seq(
        Chunk(Pos(0, 5650),Pos(314028, 45444)),
        Chunk(Pos(439897, 20150),Pos(439897, 39777))
      )
    )

    {
      val records =
        sc.loadBamIntervals(
          path,
          intervals
        )

      records.getNumPartitions should be(1)
      records.count should be(129)
    }

    {
      val records =
        sc.loadBamIntervals(
          path,
          intervals,
          splitSize = MaxSplitSize(10000)
        )

      records.getNumPartitions should be(2)
      records.count should be(129)
    }
  }

  test("1.bam") {
    import org.hammerlab.bytes._

    sc
      .loadBam(
        bam1,
        splitSize = 300 KB
      )
      .count should be(4917)
  }
}

