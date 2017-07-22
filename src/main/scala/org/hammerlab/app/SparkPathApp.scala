package org.hammerlab.app

import caseapp.Parser
import caseapp.core.Messages
import grizzled.slf4j.Logging
import org.apache.spark.{ SparkConf, SparkContext }
import org.hammerlab.io.{ Printer, SampleSize }
import org.hammerlab.spark.{ Conf, Context }

trait SparkPathAppArgs
  extends OutPathArgs {
  def printLimit: SampleSize
}

trait SparkApp[Args] {
  self: App[Args] with Logging ⇒
  private var _sparkConf: SparkConf = _
  private var _sc: SparkContext = _

  implicit def sc: SparkContext = {
    if (_sc == null) {
      _sparkConf = Conf()
      info("Creating SparkContext")
      _sc = new SparkContext(_sparkConf)
    }
    _sc
  }

  implicit def ctx: Context = sc

  override def done(): Unit = {
    if (!_sc.isStopped) {
      info("Stopping SparkContext")
      _sc.stop()
    }
    _sc = null
  }
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
