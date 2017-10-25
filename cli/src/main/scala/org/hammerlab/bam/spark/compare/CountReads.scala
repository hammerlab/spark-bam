package org.hammerlab.bam.spark.compare

import caseapp.Recurse
import org.hammerlab.args.SplitSize
import org.hammerlab.bam.spark._
import org.hammerlab.cli.app
import org.hammerlab.cli.app.{ Args, Cmd }
import org.hammerlab.cli.app.spark.PathApp
import org.hammerlab.cli.args.PrintLimitArgs
import org.hammerlab.exception.Error
import org.hammerlab.io.Printer._
import org.hammerlab.timing.Timer

object CountReads extends Cmd {
  case class Opts(@Recurse printLimit: PrintLimitArgs,
                  @Recurse splitSizeArgs: SplitSize.Args)

  val main = Main(
    args ⇒ new PathApp(args, load.Registrar)
      with Timer
      with LoadReads {

      implicit val splitSizeArgs = args.splitSizeArgs
      val splitSize = splitSizeArgs.maxSplitSize
      val (sparkBamMS, sparkBamReads) = time { sc.loadBam(path, splitSize).count }

      try {
        val (hadoopBamMS, hadoopBamReads) = time { hadoopBamLoad.count }

        echo(
          s"spark-bam read-count time: $sparkBamMS",
          s"hadoop-bam read-count time: $hadoopBamMS",
          ""
        )

        if (sparkBamReads == hadoopBamReads)
          echo(s"Read counts matched: $sparkBamReads")
        else
          echo(s"Read counts mismatched: $sparkBamReads via spark-bam, $hadoopBamReads via hadoop-bam")
      } catch {
        case e: Throwable ⇒
          echo(
            s"spark-bam found $sparkBamReads reads, hadoop-bam threw exception:",
            Error(e)
          )
      }

    }
  )
//  object Main extends app.Main(App)
}
