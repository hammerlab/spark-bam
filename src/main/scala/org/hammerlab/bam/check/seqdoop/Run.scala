package org.hammerlab.bam.check.seqdoop

import org.hammerlab.hadoop.Configuration
import org.hammerlab.bam.check.simple
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.paths.Path

object Run
  extends simple.Run {
  override def makeChecker(path: Path,
                           contigLengths: ContigLengths)(
      implicit conf: Configuration
  ): Checker =
    Checker(path, contigLengths)
}

