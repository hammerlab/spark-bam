package org.hammerlab.bam.check.eager

import org.hammerlab.bam.check.{ UncompressedStreamRun, simple }
import org.hammerlab.bam.check.simple.{ PosResult, Result }
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.block.SeekableUncompressedBytes

object Run
  extends simple.Run
    with UncompressedStreamRun[
      Boolean,
      PosResult,
      Result
    ] {
  override def makeChecker: (SeekableUncompressedBytes, ContigLengths) ⇒ Checker =
    Checker.apply
}

