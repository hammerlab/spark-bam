package org.hammerlab.bam

import caseapp._
import check.full
import org.hammerlab.bam.spark.compare
import org.hammerlab.bgzf

@AppName("spark-bam")
@ProgName("spark-bam")
sealed abstract class Command[A](val main: CaseApp[A]) {
  def args: A
}

case class         Check(@Recurse args:      check.Args) extends Command(             check.Main)
case class     FullCheck(@Recurse args:       full.Args) extends Command(              full.Main)
case class CompareSplits(@Recurse args:    compare.Opts) extends Command(           compare.Main)
case class ComputeSplits(@Recurse args:      spark.Args) extends Command(             spark.Main)
case class   IndexBlocks(@Recurse args: bgzf.index.Args) extends Command( bgzf.index.IndexBlocks)
case class  IndexRecords(@Recurse args:      index.Args) extends Command(     index.IndexRecords)
case class HtsjdkRewrite(@Recurse args:    rewrite.Args) extends Command(           rewrite.Main)

object Main
  extends CommandApp[Command[_]] {
  override def run(cmd: Command[_],
                   remainingArgs: RemainingArgs): Unit =
    cmd match {
      case c @         Check(args) ⇒ c.main.run(args, remainingArgs)
      case c @     FullCheck(args) ⇒ c.main.run(args, remainingArgs)
      case c @ CompareSplits(args) ⇒ c.main.run(args, remainingArgs)
      case c @ ComputeSplits(args) ⇒ c.main.run(args, remainingArgs)
      case c @   IndexBlocks(args) ⇒ c.main.run(args, remainingArgs)
      case c @  IndexRecords(args) ⇒ c.main.run(args, remainingArgs)
      case c @ HtsjdkRewrite(args) ⇒ c.main.run(args, remainingArgs)
    }
}
