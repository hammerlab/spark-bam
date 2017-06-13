package org.hammerlab.bgzf.hadoop

import org.apache.hadoop.conf.Configuration
import org.hammerlab.hadoop.{ FileSplits, MaxSplitSize }
import org.hammerlab.parallel
import org.hammerlab.parallel.threads

trait Config
  extends FileSplits.Config {
  def bgzfBlockHeadersToCheck: Int
  def parallelizer: parallel.Config
}

object Config {
  def apply(bgzfBlockHeadersToCheck: Int = 5,
            maxSplitSize: MaxSplitSize,
            parallelizer: parallel.Config): Config =
    ConfigImpl(
      bgzfBlockHeadersToCheck,
      maxSplitSize,
      parallelizer
    )

//  implicit def default(implicit conf: Configuration) =
//    apply(
//      parallelizer = threads.Config(8)
//    )
}

private case class ConfigImpl(bgzfBlockHeadersToCheck: Int,
                              maxSplitSize: MaxSplitSize,
                              parallelizer: parallel.Config)
  extends Config
