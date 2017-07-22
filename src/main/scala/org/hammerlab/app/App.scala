package org.hammerlab.app

import caseapp.core.Messages
import caseapp.{ CaseApp, Parser, RemainingArgs }
import grizzled.slf4j.Logging

abstract class App[Args: Parser: Messages]
  extends CaseApp[Args]
    with Logging {
  final override def run(options: Args, remainingArgs: RemainingArgs): Unit =
    try {
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
    } finally {
      done()
    }

  def done(): Unit = {}

  def run(options: Args, remainingArgs: Seq[String]): Unit
}
