package org.hammerlab.app

import caseapp.Parser
import caseapp.core.Messages
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.args.OutputArgs
import org.hammerlab.hadoop.Configuration
import org.hammerlab.io.{ Printer, SampleSize }
import org.hammerlab.spark.{ SparkConfBase, confs }

trait SparkPathAppArgs {
  def output: OutputArgs
}

trait HasSparkConf
  extends SparkConfBase
    with confs.Kryo
    with confs.DynamicAllocation
    with confs.EventLog
    with confs.Speculation

trait SparkApp[Args]
  extends HasSparkConf {

  self: App[Args] with Logging ⇒

  @transient private var _sc: SparkContext = _

  implicit def sc: SparkContext = {
    if (_sc == null) {
      info("Creating SparkContext")
      _sc = new SparkContext(makeSparkConf)
    }
    _sc
  }

  implicit def conf: Configuration = sc.hadoopConfiguration

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
abstract class SparkPathApp[Args <: SparkPathAppArgs : Parser : Messages](override val registrar: Class[_ <: KryoRegistrator])
  extends PathApp[Args]
    with SparkApp[Args] {

  @transient implicit var printer: Printer = _
  @transient implicit var printLimit: SampleSize = _

  override def init(options: Args): Unit = {
    val OutputArgs(printLim, path, overwrite) = options.output

    if (path.exists(_.exists) && !overwrite)
      throw new IllegalArgumentException(
        s"Output path $path exists and overwrite (-f) not set"
      )

    printer = Printer(path)
    printLimit = printLim
  }
}
