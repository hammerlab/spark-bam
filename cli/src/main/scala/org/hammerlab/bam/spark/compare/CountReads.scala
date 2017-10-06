package org.hammerlab.bam.spark.compare

import caseapp.Recurse
import org.hammerlab.args.SplitSize
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.spark._
import org.hammerlab.cli.app.{ SparkPathApp, SparkPathAppArgs }
import org.hammerlab.cli.args.OutputArgs
import org.hammerlab.exception.Error
import org.hammerlab.io.Printer._
import org.hammerlab.timing.Timer

case class CountReadsArgs(@Recurse output: OutputArgs,
                          @Recurse splitSizeArgs: SplitSize.Args)
  extends SparkPathAppArgs

object CountReads
  extends SparkPathApp[CountReadsArgs](classOf[Registrar])
    with Timer
    with LoadReads {
  override protected def run(args: CountReadsArgs): Unit = {
    implicit val splitSizeArgs = args.splitSizeArgs
    implicit val splitSize = splitSizeArgs.maxSplitSize
    val (sparkBamMS, sparkBamReads) = time { sc.loadBam(path).count }

    try {
      val (hadoopBamMS, hadoopBamReads) =
        time {
          hadoopBamLoad.count
        }

      echo(
        s"Spark-bam read-count time: $sparkBamMS",
        s"Hadoop-bam read-count time: $hadoopBamMS",
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
}
