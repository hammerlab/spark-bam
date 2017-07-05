package org.hammerlab.bam.spark.load

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.spark._
import org.hammerlab.genomics.loci.set.test.LociSetUtil
import org.hammerlab.hadoop.splits.MaxSplitSize
import org.hammerlab.magic.rdd.partitions.PartitionSizesRDD._
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File

trait LoadBAMChecks
  extends SparkSuite
    with LociSetUtil {

  def file: String

  def path = File(file).path

  def load(maxSplitSize: MaxSplitSize): RDD[SAMRecord]

  def check(maxSplitSize: MaxSplitSize, sizes: Int*): Unit = {
    val records = load(maxSplitSize)

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
}
