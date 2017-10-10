package org.hammerlab.bam

import caseapp._
import org.hammerlab.bam.check.{ blocks, eager, full }
import org.hammerlab.bam.spark.compare
import org.hammerlab.bam.spark.compare.{ CountReadsArgs, TimeLoadArgs }
import org.hammerlab.bgzf

@AppName("spark-bam")
@ProgName("spark-bam")
sealed abstract class Command[A](val main: CaseApp[A]) {
  def args: A
}

case class      CheckBam(@Recurse args:      eager.Args) extends Command(             eager.Main)
case class   CheckBlocks(@Recurse args:      eager.Args) extends Command(            blocks.Main)
case class    CountReads(@Recurse args:  CountReadsArgs) extends Command(     compare.CountReads)
case class      TimeLoad(@Recurse args:    TimeLoadArgs) extends Command(       compare.TimeLoad)
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
