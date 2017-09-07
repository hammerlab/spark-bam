package org.hammerlab.bgzf

import caseapp.core.{ ArgParser, Default }
import caseapp.core.ArgParser.instance

package object block {
  // TODO: move this elsewhere
  trait IntWrapper extends Any {
    def n: Int
    override def toString: String = n.toString
  }

  implicit class BGZFBlocksToCheck(val n: Int) extends AnyVal with IntWrapper
  object BGZFBlocksToCheck {
    implicit val parser: ArgParser[BGZFBlocksToCheck] =
      instance("bgzf-blocks-to-check") {
        str ⇒ Right(str.toInt)
      }

    implicit val default: Default[BGZFBlocksToCheck] =
      Default.instance(5)
  }
}
