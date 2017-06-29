package org.hammerlab.bam

import org.apache.spark.SparkContext
import org.hammerlab.bam.spark.LoadBam.CanLoadBam

package object spark {
  implicit class LoadBamContext(val sc: SparkContext)
    extends AnyVal
      with CanLoadBam

  implicit val defaultPartitioningStrategy = LoadBam.defaultPartitioningStrategy
}
