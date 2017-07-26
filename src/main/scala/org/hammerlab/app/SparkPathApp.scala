package org.hammerlab.app

import caseapp.Parser
import caseapp.core.Messages
import grizzled.slf4j.Logging
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{ SparkConf, SparkContext }
import org.hammerlab.io.{ Printer, SampleSize }
import org.hammerlab.spark.{ Conf, Context, DynamicAllocationConfs, EventLogConfs, KryoConfs, SparkConfBase, SpeculationConfs }

trait SparkPathAppArgs
  extends OutPathArgs {
  def printLimit: SampleSize
}

trait HasSparkConf
  extends SparkConfBase
    with KryoConfs
    with DynamicAllocationConfs
    with EventLogConfs
    with SpeculationConfs

trait SparkApp[Args]
  extends HasSparkConf {
  self: App[Args] with Logging ⇒

  private var _sc: SparkContext = _

  implicit def sc: SparkContext = {
    if (_sc == null) {
      info("Creating SparkContext")
      _sc = new SparkContext(makeSparkConf)
    }
    _sc
  }

  implicit def ctx: Context = sc

  override def done(): Unit = {
    if (_sc != null && !_sc.isStopped) {
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
abstract class SparkPathApp[Args <: SparkPathAppArgs : Parser : Messages ](override val registrar: Class[_ <: KryoRegistrator])
  extends PathApp[Args]
    with SparkApp[Args]{

  implicit var printer: Printer = _
  implicit var printLimit: SampleSize = _

  override def init(options: Args): Unit = {
    printer = Printer(options.out)
    printLimit = options.printLimit
  }
}
