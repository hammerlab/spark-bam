package org.hammerlab.bam.check.eager

import org.hammerlab.bam.check.simple.Result
import org.hammerlab.bam.check.{ Args, simple }
import org.hammerlab.hadoop.Path

class RunTest
  extends simple.RunTest {
  override def run(args: Args)(implicit path: Path): Result =
    Run(args)
}
