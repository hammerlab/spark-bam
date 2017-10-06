package org.hammerlab.bam.spark

import caseapp.CaseApp
import org.hammerlab.paths.Path
import org.hammerlab.spark.test.suite
import org.hammerlab.test.firstLinesMatch
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.matchers.lines.Line
import org.hammerlab.test.resources.File

abstract class MainSuite(app: CaseApp[_])
  extends suite.MainSuite {
  def defaultOpts(outPath: Path): Seq[Arg] = Nil
  def defaultArgs(outPath: Path): Seq[Arg] = Nil

  case class Arg(override val toString: String)
  implicit def strArg(s: String): Arg = Arg(s)
  implicit def pathArg(path: Path): Arg = Arg(path.toString)

  def checkLines(args: Arg*)(lines: Line*): Unit = {
    val outPath = tmpPath()

    app.main(
      defaultOpts(outPath).map(_.toString).toArray ++
        args.map(_.toString) ++
        defaultArgs(outPath).map(_.toString)
    )

    outPath.read should firstLinesMatch(lines: _*)
  }

  def checkFile(args: Arg*)(expectedFile: File): Unit = {
    val outPath = tmpPath()

    app.main(
      defaultOpts(outPath).map(_.toString).toArray ++
        args.map(_.toString) ++
        defaultArgs(outPath).map(_.toString)
    )

    outPath should fileMatch(expectedFile)
  }

  def check(args: Arg*)(expected: String): Unit = {
    val outPath = tmpPath()

    app.main(
      defaultOpts(outPath).map(_.toString).toArray ++
        args.map(_.toString) ++
        defaultArgs(outPath).map(_.toString)
    )

    outPath.read should be(expected.stripMargin)
  }
}
