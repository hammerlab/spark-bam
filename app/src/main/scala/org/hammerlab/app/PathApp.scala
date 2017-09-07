package org.hammerlab.app

import java.io.Closeable

import caseapp.Parser
import caseapp.core.Messages
import org.hammerlab.paths.Path

abstract class PathApp[Args : Parser : Messages]
  extends App[Args]
    with Closeable {

  @transient implicit var path: Path = _

  def init(options: Args): Unit = {}
  def close(): Unit = {}

  final protected override def _run(options: Args, args: Seq[String]): Unit = {
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

  protected def run(options: Args): Unit
}
