package org.hammerlab.bam.spark.load

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.spark._
import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.bam.spark.load.CanLoadBam.getIntevalChunks
import org.hammerlab.bgzf.Pos
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.parallel.spark.ElemsPerPartition

class LoadBAMThreads
  extends LoadBAMTest {
  override val parallelConfig: ParallelConfig =
    Threads(4)
}

class LoadBAMSpark
  extends LoadBAMTest {
  lazy val parallelConfig: ParallelConfig =
    Spark(ElemsPerPartition(1))
}

trait LoadBAMTest
  extends LoadBAMChecks {

  def parallelConfig: ParallelConfig

  override val file = "5k.bam"

  override def load(maxSplitSize: MaxSplitSize): RDD[SAMRecord] =
    sc
      .loadReads(
        path,
        parallelConfig = parallelConfig,
        splitSize = maxSplitSize
      )

  test("1e6") {
    check(
      MaxSplitSize(1000000),
      4910
    )
  }

  test("1e5") {
    check(
      MaxSplitSize(100000),
      507, 515, 409, 507, 501,
      510, 517, 420, 509, 492,
      23
    )
  }

  test("1e4") {
    check(
      MaxSplitSize(10000),
      104, 101, 104, 100,  98, 101, 100, 105, 105, 104,
      101, 102, 102, 104, 105, 105, 104,  97,  96,  96,
       97,  99, 104, 105, 102, 101, 100, 104, 103, 105,
       99, 103, 105, 105, 105, 105, 105, 105, 103, 104,
      105, 101,  96,  99,  99,  98,  98,  98,  23
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
          Pos(   2454,     0),
          Pos(1010675,     0)
        )
      )
    )

    val records =
      sc.loadBamIntervals(path, intervals)

    records.count should be(4787)  // 4910 reads, 23 unmapped
  }

  test("indexed disjoint regions") {
    val intervals: LociSet = "1:13000-14000,1:60000-61000"

    getIntevalChunks(
      path,
      intervals
    ) should be(
      Seq(
        Chunk(
          Pos(  2454,     0),
          Pos(284685, 33959)
        ),
        Chunk(
          Pos(905238, 63468),
          Pos(928569, 18303)
        )
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
}

