package org.hammerlab.args

import caseapp.{ ValueDescription, ExtraName ⇒ O, HelpMessage ⇒ M }
import org.hammerlab.io.SampleSize
import org.hammerlab.paths.Path

case class OutputArgs(
  @O("l")
  @ValueDescription("num")
  @M("When collecting samples of records/results for displaying to the user, limit to this many to avoid overloading the driver")
  printLimit: SampleSize = SampleSize(1000000),

  @O("o")
  @ValueDescription("path")
  @M("Print output to this file, otherwise to stdout")
  outputPath: Option[Path] = None
)
