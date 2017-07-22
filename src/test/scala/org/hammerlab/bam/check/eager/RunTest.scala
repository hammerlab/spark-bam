package org.hammerlab.bam.check.eager

import org.hammerlab.bam.check.{ Args, Result, simple }
import org.hammerlab.paths.Path

class RunTest
  extends simple.RunTest {
  override def run(args: Args)(implicit path: Path): Result =
    Run(args)
}
