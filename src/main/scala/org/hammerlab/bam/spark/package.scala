package org.hammerlab.bam

import org.hammerlab.bam.spark.LoadBam.CanLoadBam
import org.hammerlab.spark.Context

package object spark {
  implicit class LoadBamContext(val sc: Context)
    extends AnyVal
      with CanLoadBam

  implicit val defaultPartitioningStrategy = LoadBam.defaultPartitioningStrategy
}
