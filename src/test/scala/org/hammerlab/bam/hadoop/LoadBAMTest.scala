package org.hammerlab.bam.hadoop

import org.hammerlab.bam.hadoop.LoadBam._
import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.bgzf.Pos
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.loci.set.test.LociSetUtil
import org.hammerlab.hadoop.{ MaxSplitSize, Path }
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File

class LoadBAMTest
  extends SparkSuite
    with LociSetUtil {

  def check(maxSplitSize: MaxSplitSize, sizes: Int*)(implicit path: Path): Unit = {
    val records =
      sc
        .loadBam(
          path
        )(
          Config(maxSplitSize = maxSplitSize)
        )

    records.partitionSizes should be(sizes)
    records.count should be(4910)
    records.map(_.getReadName).take(10) should be(
      Array(
        "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724",
        "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724",
        "HWI-ST807:461:C2P0JACXX:4:1304:9505:89866",
        "HWI-ST807:461:C2P0JACXX:4:2311:6431:65669",
        "HWI-ST807:461:C2P0JACXX:4:1305:2342:51860",
        "HWI-ST807:461:C2P0JACXX:4:1305:2342:51860",
        "HWI-ST807:461:C2P0JACXX:4:1304:9505:89866",
        "HWI-ST807:461:C2P0JACXX:4:2311:6431:65669",
        "HWI-ST807:461:C2P0JACXX:4:1107:13461:64844",
        "HWI-ST807:461:C2P0JACXX:4:2203:17157:59976"
      )
    )
  }

  implicit lazy val config = Config(maxSplitSize = MaxSplitSize())

  {
    implicit val path = Path(File("5k.bam").uri)

    test("5k.bam 1e6") {
      check(
        maxSplitSize = 1000000L,
        4910
      )
    }

    test("5k.bam 1e5") {
      check(
        maxSplitSize = 100000L,
        507, 515, 409, 507, 501, 510, 517, 420, 509, 492, 23
      )
    }

    test("5k.bam 1e4") {
      check(
        maxSplitSize = 10000L,
        104, 101, 104, 100, 98, 101, 100, 105, 105, 104,
        101, 102, 102, 104, 105, 105, 104, 97, 96, 96,
        97, 99, 104, 105, 102, 101, 100, 104, 103, 105,
        99, 103, 105, 105, 105, 105, 105, 105, 103, 104,
        105, 101, 96, 99, 99, 98, 98, 98, 23
      )
    }

    test("5k.bam indexed all") {
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

    test("5k.bam indexed disjoint regions") {
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
        val records = sc.loadBamIntervals(path, intervals)

        records.getNumPartitions should be(1)
        records.count should be(129)
      }

      {
        val records = sc.loadBamIntervals(path, intervals)(Config(maxSplitSize = MaxSplitSize(10000)))

        records.getNumPartitions should be(2)
        records.count should be(129)
      }
    }
  }

  {
    implicit val path = Path(File("5k.sam").uri)

    test("5k.sam 4MM") {
      check(
        maxSplitSize = 4000000L,
        4910
      )
    }

    test("5k.sam 3MM") {
      check(
        maxSplitSize = 3000000L,
        2447, 2463
      )
    }

    test("5k.sam 1MM") {
      check(
        maxSplitSize = 1000000L,
        1226, 1221, 1244, 1219
      )
    }

    test("5k.sam intervals") {
      val intervals: LociSet = "1:13000-14000,1:60000-61000"

      val records = sc.loadBamIntervals(path, intervals)

      records.count should be(129)
    }
  }
}
