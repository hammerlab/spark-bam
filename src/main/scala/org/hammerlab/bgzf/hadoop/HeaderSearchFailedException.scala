package org.hammerlab.bgzf.hadoop

import java.io.IOException

import org.hammerlab.paths.Path

case class HeaderSearchFailedException(path: Path,
                                       start: Long,
                                       positionsAttempted: Int)
  extends IOException(
    s"$path: failed to find BGZF header in $positionsAttempted bytes from $start"
  )
