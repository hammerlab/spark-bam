package org.hammerlab.app

import caseapp.{ CaseApp, Parser, RemainingArgs }
import caseapp.core.Messages
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.hammerlab.io.{ Printer, SampleSize }
import org.hammerlab.paths.Path
import org.hammerlab.spark.{ Conf, Context }

abstract class App[Args: Parser: Messages]
  extends CaseApp[Args]
    with Logging {
  final override def run(options: Args, remainingArgs: RemainingArgs): Unit =
    remainingArgs match {
      case RemainingArgs(args, Nil) ⇒
        run(
          options,
          args
        )
      case RemainingArgs(args, unparsed) ⇒
        throw new IllegalArgumentException(
          s"Unparsed arguments: ${unparsed.mkString(" ")}"
        )
    }

  def run(options: Args, remainingArgs: Seq[String]): Unit
}
