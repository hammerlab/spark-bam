package org.hammerlab.bam.check

import org.apache.log4j.Level.WARN
import org.apache.log4j.Logger.getRootLogger
import org.apache.spark.SparkContext
import org.hammerlab.args.{ ByteRanges, FindReadArgs, LogArgs }
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.kryo._
import org.hammerlab.paths.Path
import shapeless._
import shapeless.ops.hlist.Selector

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
    selectBlocks: Selector[L, Blocks.Args],
    selectIndexedRecordPositions: Selector[L, IndexedRecordPositions.Args],
    selectLog: Selector[L, LogArgs],
    selectFindReads: Selector[L, FindReadArgs]
)
  extends Serializable {

  private val g = gen.to(t)

  implicit val blocksArgs = g.select[Blocks.Args]
  implicit val records = g.select[IndexedRecordPositions.Args]
  implicit val logging = g.select[LogArgs]
  implicit val findReadArgs = g.select[FindReadArgs]

  val header = Header(path)
  implicit val headerBroadcast = sc.broadcast(header)
  implicit val contigLengthsBroadcast = sc.broadcast(header.contigLengths)
  implicit val rangesBroadcast = sc.broadcast(blocksArgs.ranges)

  implicit val readsToCheck: ReadsToCheck = findReadArgs.readsToCheck
  implicit val maxReadSize: MaxReadSize = findReadArgs.maxReadSize

  if (logging.warn)
    getRootLogger.setLevel(WARN)

  def run(): Unit

  run()
}

object CheckerMain extends spark.Registrar(
  cls[Header],
  cls[ContigLengths],
  cls[Option[_]],
  cls[ByteRanges]
)
