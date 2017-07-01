package org.hammerlab.bgzf.hadoop

import org.hammerlab.hadoop.splits.{ FileSplits, MaxSplitSize }
import org.hammerlab.parallel

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
}

private case class ConfigImpl(bgzfBlockHeadersToCheck: Int,
                              maxSplitSize: MaxSplitSize,
                              parallelizer: parallel.Config)
  extends Config
