package org.hammerlab.bam

import caseapp.core.ArgParser.instance
import caseapp.core.{ ArgParser, Default }
import org.hammerlab.bgzf.block.IntWrapper

package object check {
  implicit class SuccessfulReads(val n: Int) extends AnyVal with IntWrapper

  implicit class ReadsToCheck(val n: Int) extends AnyVal with IntWrapper
  object ReadsToCheck {
    implicit val parser: ArgParser[ReadsToCheck] =
      instance("reads-to-check") {
        str ⇒ Right(str.toInt)
      }

    implicit val default: Default[ReadsToCheck] =
      Default.instance(10)
  }

  implicit class MaxReadSize(val n: Int) extends AnyVal with IntWrapper
  object MaxReadSize {
    implicit val parser: ArgParser[MaxReadSize] =
      instance[MaxReadSize]("max-read-size") {
        str ⇒ Right(str.toInt)
      }

    implicit val default: Default[MaxReadSize] =
      Default.instance(100000000)
  }
}
