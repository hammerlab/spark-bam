package org.hammerlab.bam

import caseapp.core.argparser._
import caseapp.core.default.Default
import org.hammerlab.bgzf.block.IntWrapper

package object check {
  implicit class SuccessfulReads(val n: Int) extends AnyVal with IntWrapper

  implicit class ReadsToCheck(val n: Int) extends AnyVal with IntWrapper
  object ReadsToCheck {
    implicit val parser: ArgParser[ReadsToCheck] =
      SimpleArgParser.from("reads-to-check") {
        str ⇒ Right(str.toInt)
      }

    implicit val default: ReadsToCheck = 10
    implicit val defaultParam = Default(default)
  }

  implicit class MaxReadSize(val n: Int) extends AnyVal with IntWrapper
  object MaxReadSize {
    implicit val parser: ArgParser[MaxReadSize] =
      SimpleArgParser.from[MaxReadSize]("max-read-size") {
        str ⇒ Right(str.toInt)
      }

    implicit val default: MaxReadSize = 100000000
    implicit val defaultParam = Default(default)
  }
}
