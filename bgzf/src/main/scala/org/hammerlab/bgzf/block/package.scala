package org.hammerlab.bgzf

import caseapp.core.default.Default
import caseapp.core.argparser._

package object block {
  // TODO: move this elsewhere
  trait IntWrapper extends Any {
    def n: Int
    override def toString: String = n.toString
  }

  implicit class BGZFBlocksToCheck(val n: Int) extends AnyVal with IntWrapper
  object BGZFBlocksToCheck {
    implicit val parser: ArgParser[BGZFBlocksToCheck] =
      SimpleArgParser.from("bgzf-blocks-to-check") {
        str ⇒ Right(str.toInt)
      }

    implicit val default: Default[BGZFBlocksToCheck] = Default(5)
  }
}
