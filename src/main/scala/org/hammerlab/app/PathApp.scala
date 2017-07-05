package org.hammerlab.app

import java.io.Closeable

import caseapp.Parser
import caseapp.core.Messages
import org.hammerlab.paths.Path

abstract class PathApp[Args : Parser : Messages]
  extends App[Args]
    with Closeable {

  implicit var path: Path = _

  def init(options: Args): Unit = {}
  def close(): Unit = {}

  final override def run(options: Args, args: Seq[String]): Unit = {
    if (args.size != 1) {
      throw new IllegalArgumentException(
        s"Exactly one argument (a BAM file path) is required"
      )
    }

    path = Path(args.head)

    init(options)
    run(options)
    close()
  }

  def run(options: Args): Unit
}
