package org.hammerlab.bam.hadoop

import org.hammerlab.bam.hadoop.LoadBam._
import org.hammerlab.hadoop.Path
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File

class LoadBAMTest
  extends SparkSuite {

  def check(maxSplitSize: Int, sizes: Int*): Unit = {
    val records =
      sc
        .loadBam(
          Path(File("5k.bam").uri)
        )(
          Config(maxSplitSize = Some(maxSplitSize))
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

  test("5k.bam 1e6") {
    check(1000000, 4910)
  }

  test("5k.bam 1e5") {
    check(
      100000,
      507, 515, 409, 507, 501, 510, 517, 420, 509, 492, 23
    )
  }

  test("5k.bam 1e4") {
    check(
      10000,
      104, 101, 104, 100,  98, 101, 100, 105, 105, 104,
      101, 102, 102, 104, 105, 105, 104,  97,  96,  96,
       97,  99, 104, 105, 102, 101, 100, 104, 103, 105,
       99, 103, 105, 105, 105, 105, 105, 105, 103, 104,
      105, 101, 96, 99, 99, 98, 98, 98, 23
    )
  }
}
