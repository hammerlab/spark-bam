package org.hammerlab.bam.check.eager

import org.hammerlab.bam.check.simple.Result
import org.hammerlab.bam.check.{ Args, simple }

class RunTest
  extends simple.RunTest {
  override def run(args: Args): Result =
    Run(sc, args)
}
