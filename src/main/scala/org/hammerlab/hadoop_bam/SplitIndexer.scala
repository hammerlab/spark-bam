package org.hammerlab.hadoop_bam

import org.apache.hadoop.fs.Path
import org.seqdoop.hadoop_bam.SplittingBAMIndexer.DEFAULT_GRANULARITY

class SplitIndexer {

}

object SplitIndexer {
  def apply(path: Path, outputOffsetEvery: Int = DEFAULT_GRANULARITY): Unit = {

  }
}
