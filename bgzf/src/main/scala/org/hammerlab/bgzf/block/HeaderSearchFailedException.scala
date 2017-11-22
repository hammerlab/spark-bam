package org.hammerlab.bgzf.block

import java.io.IOException

import hammerlab.path._

case class HeaderSearchFailedException(path: Path,
                                       start: Long,
                                       positionsAttempted: Int)
  extends IOException(
    s"$path: failed to find BGZF header in $positionsAttempted bytes from $start"
  )
