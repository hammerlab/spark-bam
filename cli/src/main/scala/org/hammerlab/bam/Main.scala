package org.hammerlab.bam

import caseapp._
import org.hammerlab.bam.check.{ blocks, eager, full }
import org.hammerlab.bam.spark.compare
import org.hammerlab.bgzf

@AppName("spark-bam")
@ProgName("spark-bam")
sealed abstract class Command[A](val main: CaseApp[A]) {
  def args: A
}

trait Cmd {
  type Main
  type App
  type Opts
}

case class      CheckBam(@Recurse args:         eager.CheckBam.Opts) extends Command(             eager.CheckBam.Main)
case class   CheckBlocks(@Recurse args:     blocks.CheckBlocks.Opts) extends Command(            blocks.CheckBlocks.Main)
case class    CountReads(@Recurse args:     compare.CountReads.Opts) extends Command(     compare.CountReads.Main)
case class      TimeLoad(@Recurse args:       compare.TimeLoad.Opts) extends Command(       compare.TimeLoad.Main)
case class     FullCheck(@Recurse args:         full.FullCheck.Opts) extends Command(              full.FullCheck.Main)
case class CompareSplits(@Recurse args:  compare.CompareSplits.Opts) extends Command(           compare.CompareSplits.Main)
case class ComputeSplits(@Recurse args:    spark.ComputeSplits.Opts) extends Command(             spark.ComputeSplits.Main)
case class   IndexBlocks(@Recurse args: bgzf.index.IndexBlocks.Opts) extends Command( bgzf.index.IndexBlocks.Main)
case class  IndexRecords(@Recurse args:     index.IndexRecords.Opts) extends Command(     index.IndexRecords.Main)
case class HtsjdkRewrite(@Recurse args:  rewrite.HTSJDKRewrite.Opts) extends Command(           rewrite.HTSJDKRewrite.Main)

object Main
  extends CommandApp[Command[_]] {
  override def run(cmd: Command[_],
                   remainingArgs: RemainingArgs): Unit =
    cmd match {
      case c @      CheckBam(args) ⇒ c.main.run(args, remainingArgs)
      case c @   CheckBlocks(args) ⇒ c.main.run(args, remainingArgs)
      case c @    CountReads(args) ⇒ c.main.run(args, remainingArgs)
      case c @      TimeLoad(args) ⇒ c.main.run(args, remainingArgs)
      case c @     FullCheck(args) ⇒ c.main.run(args, remainingArgs)
      case c @ CompareSplits(args) ⇒ c.main.run(args, remainingArgs)
      case c @ ComputeSplits(args) ⇒ c.main.run(args, remainingArgs)
      case c @   IndexBlocks(args) ⇒ c.main.run(args, remainingArgs)
      case c @  IndexRecords(args) ⇒ c.main.run(args, remainingArgs)
      case c @ HtsjdkRewrite(args) ⇒ c.main.run(args, remainingArgs)
    }
}
