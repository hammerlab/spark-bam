package org.hammerlab.bam

import caseapp.{ Recurse ⇒ R, _ }
import org.hammerlab.bam.check.{ blocks, eager, full }
import org.hammerlab.bam.spark.compare
import org.hammerlab.bgzf
import org.hammerlab.cli.app.Cmd

sealed abstract class Command[+C <: Cmd](val cmd: C) {
  def opts: cmd.Opts
  def apply(opts: cmd.Opts,
            remainingArgs: RemainingArgs): Unit =
    cmd.main.run(
      opts,
      remainingArgs
    )
}

case class      CheckBam(@R opts:         eager.CheckBam.Opts) extends Command(         eager.CheckBam)
case class   CheckBlocks(@R opts:     blocks.CheckBlocks.Opts) extends Command(     blocks.CheckBlocks)
case class    CountReads(@R opts:     compare.CountReads.Opts) extends Command(     compare.CountReads)
case class      TimeLoad(@R opts:       compare.TimeLoad.Opts) extends Command(       compare.TimeLoad)
case class     FullCheck(@R opts:         full.FullCheck.Opts) extends Command(         full.FullCheck)
case class CompareSplits(@R opts:  compare.CompareSplits.Opts) extends Command(  compare.CompareSplits)
case class ComputeSplits(@R opts:    spark.ComputeSplits.Opts) extends Command(    spark.ComputeSplits)
case class   IndexBlocks(@R opts: bgzf.index.IndexBlocks.Opts) extends Command( bgzf.index.IndexBlocks)
case class  IndexRecords(@R opts:     index.IndexRecords.Opts) extends Command(     index.IndexRecords)
case class HtsjdkRewrite(@R opts:  rewrite.HTSJDKRewrite.Opts) extends Command(  rewrite.HTSJDKRewrite)

object Main
  extends CommandApp[Command[Cmd]] {

  override val    appName = "spark-bam"
  override val appVersion = "1.2.0-M1"
  override val   progName = "spark-bam"

  override def run(cmd: Command[Cmd],
                   remainingArgs: RemainingArgs): Unit =
    cmd(
      cmd.opts,
      remainingArgs
    )
}
