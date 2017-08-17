package org.hammerlab.args

import caseapp.{ ExtraName ⇒ O, HelpMessage ⇒ M }
import org.hammerlab.bytes.Bytes
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.MaxSplitSize

object SplitSize {
  case class Args(
    @O("m")
    @M("Maximum Hadoop split-size; if unset, default to underlying FileSystem's value")
    splitSize: Option[Bytes]
  ) {
    def maxSplitSize(implicit conf: Configuration): MaxSplitSize =
      MaxSplitSize(splitSize)

    def maxSplitSize(default: Bytes): MaxSplitSize =
      MaxSplitSize(splitSize.getOrElse(default))
  }
}
