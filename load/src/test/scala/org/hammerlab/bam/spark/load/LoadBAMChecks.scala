package org.hammerlab.bam.spark.load

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.test.resources.TestBams
import org.hammerlab.genomics.loci.set.test.LociSetUtil
import org.hammerlab.hadoop.splits.MaxSplitSize
import magic_rdds.partitions._
import org.hammerlab.magic.rdd.partitions.PartitionSizes
import org.hammerlab.spark.test.suite.KryoSparkSuite

trait LoadBAMChecks
  extends KryoSparkSuite
    with LociSetUtil
    with TestBams {

  register(
    new Registrar,

    PartitionSizes,

    /** We [[RDD.take]] some read-names below */
    classOf[Array[String]]
  )

  def load(maxSplitSize: MaxSplitSize): RDD[SAMRecord]

  def check(maxSplitSize: MaxSplitSize, sizes: Int*): Unit = {
    val records = load(maxSplitSize)

    records.partitionSizes should be(sizes)
    records.count should be(2500)
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
