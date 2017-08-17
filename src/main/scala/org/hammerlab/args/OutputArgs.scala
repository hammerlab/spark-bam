package org.hammerlab.args

import caseapp.{ ExtraName ⇒ O, HelpMessage ⇒ M }
import org.hammerlab.io.SampleSize
import org.hammerlab.paths.Path

case class OutputArgs(
  @O("l")
  @M("When collecting samples of records/results for displaying to the user, limit to this many to avoid overloading the driver")
  printLimit: SampleSize = SampleSize(1000000),

  @O("o")
  @M("Print output to this file, otherwise to stdout")
  path: Option[Path] = None
)
