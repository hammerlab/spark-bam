package org.hammerlab.bam.spark

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD

case class BAMRecordRDD(splits: Seq[Split],
                        reads: RDD[SAMRecord])

object BAMRecordRDD {
  implicit def SAMRecordRDDToRDD(samRecordRDD: BAMRecordRDD): RDD[SAMRecord] =
    samRecordRDD.reads
}
