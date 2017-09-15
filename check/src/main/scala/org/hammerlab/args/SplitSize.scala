package org.hammerlab.args

import caseapp.{ ValueDescription, ExtraName ⇒ O, HelpMessage ⇒ M }
import org.hammerlab.bytes.Bytes
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.MaxSplitSize

object SplitSize {
  case class Args(
    @O("max-split-size") @O("m")
    @ValueDescription("bytes")
    @M("Maximum Hadoop split-size; if unset, default to underlying FileSystem's value. Integers as well as byte-size short-hands accepted, e.g. 64m, 32MB")
    splitSize: Option[Bytes]
  ) {
    def maxSplitSize(implicit conf: Configuration): MaxSplitSize =
      MaxSplitSize(splitSize)

    def maxSplitSize(default: Bytes): MaxSplitSize =
      MaxSplitSize(splitSize.getOrElse(default))
  }
}
