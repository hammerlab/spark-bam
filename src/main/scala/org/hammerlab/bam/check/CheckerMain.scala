package org.hammerlab.bam.check

import org.apache.log4j.Level.WARN
import org.apache.log4j.Logger.getRootLogger
import org.apache.spark.SparkContext
import org.hammerlab.args.LogArgs
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.header.Header
import org.hammerlab.paths.Path
import shapeless._
import shapeless.ops.hlist.Take

/**
 * Container for extracting some common types from arguments and a target BAM file and making them implicitly available
 * in subclasses.
 *
 * @tparam Args arguments case-class that must start with Blocks.Args, IndexedRecordPositions.Args, and LogArgs fields
 */
abstract class CheckerMain[Args, L <: HList](t: Args)(
    implicit
    sc: SparkContext,
    path: Path,
    gen: Generic.Aux[Args, L],
    take: Take.Aux[L, Nat._3, Blocks.Args :: IndexedRecordPositions.Args :: LogArgs :: HNil]
)
  extends Serializable {

  private val g = gen.to(t)
  private val fields = take.apply(g)

  implicit val blocksArgs: Blocks.Args = fields.head
  implicit val records: IndexedRecordPositions.Args = fields(Nat._1)
  implicit val logging: LogArgs = fields(Nat._2)

  val header = Header(path)
  implicit val headerBroadcast = sc.broadcast(header)
  implicit val contigLengthsBroadcast = sc.broadcast(header.contigLengths)
  implicit val rangesBroadcast = sc.broadcast(blocksArgs.ranges)

  if (logging.warn)
    getRootLogger.setLevel(WARN)

  def run(): Unit

  run()
}
