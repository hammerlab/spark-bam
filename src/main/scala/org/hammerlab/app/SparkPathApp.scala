package org.hammerlab.app

import caseapp.Parser
import caseapp.core.Messages
import org.apache.spark.SparkContext
import org.hammerlab.io.{ Printer, SampleSize }
import org.hammerlab.spark.{ Conf, Context }

trait SparkPathAppArgs
  extends OutPathArgs {
  def printLimit: SampleSize
}

trait SparkApp[Args] {
  self: App[Args] ⇒
  private val sparkConf = Conf()
  implicit val sc: SparkContext = new SparkContext(sparkConf)
  implicit val ctx: Context = sc
}

/**
 * [[SparkApp]] that takes an input path and prints some information to stdout or a path, with optional truncation of
 * such output.
 */
abstract class SparkPathApp[Args <: SparkPathAppArgs : Parser : Messages ]
  extends PathApp[Args]
    with SparkApp[Args]{

  implicit var printer: Printer = _
  implicit var printLimit: SampleSize = _

  override def init(options: Args): Unit = {
    printer = Printer(options.out)
    printLimit = options.printLimit
  }
}
