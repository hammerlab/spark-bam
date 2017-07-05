package org.hammerlab.bam

import org.apache.spark.SparkContext
import org.hammerlab.bam.spark.load.CanLoadBam

package object spark {
  implicit class LoadBamContext(val sc: SparkContext)
    extends CanLoadBam
}
