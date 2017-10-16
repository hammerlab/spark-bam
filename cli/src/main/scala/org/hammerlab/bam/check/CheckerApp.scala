package org.hammerlab.bam.check

import org.apache.log4j.Level.WARN
import org.apache.log4j.Logger.getRootLogger
import org.apache.spark.SparkContext
import org.hammerlab.args.{ ByteRanges, FindReadArgs, LogArgs }
import org.hammerlab.bam.check.indexed.IndexedRecordPositions
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.cli.app.Args
import org.hammerlab.cli.app.spark.{ PathApp, Registrar }
import org.hammerlab.kryo._
import org.hammerlab.paths.Path
import org.hammerlab.shapeless.hlist.Find
import org.hammerlab.shapeless.record.Field
import shapeless._
import shapeless.ops.hlist.Selector

/**
 * Container for extracting some common types from arguments and a target BAM file and making them implicitly available
 * in subclasses.
 */
abstract class CheckerApp[Opts](args: Args[Opts], reg: Registrar)(
    implicit
    findBlocks: Find[Opts, Blocks.Args],
    findIndexedRecordPositions: Find[Opts, IndexedRecordPositions.Args],
    findLog: Find[Opts, LogArgs],
    findFindReads: Find[Opts, FindReadArgs]
)
  extends PathApp(args, reg) {

//  private val g = gen.to(t)

  implicit val blocksArgs = findBlocks(args)
  implicit val recordPosArgs: IndexedRecordPositions.Args = findIndexedRecordPositions(args)
  implicit val logging = findLog(args)
  implicit val findReadArgs = findFindReads(args)

  val header = Header(path)
  implicit val headerBroadcast = sc.broadcast(header)
  implicit val contigLengthsBroadcast = sc.broadcast(header.contigLengths)
  implicit val rangesBroadcast = sc.broadcast(blocksArgs.ranges)

  implicit val readsToCheck: ReadsToCheck = findReadArgs.readsToCheck
  implicit val maxReadSize: MaxReadSize = findReadArgs.maxReadSize

  if (logging.warn)
    getRootLogger.setLevel(WARN)
}

object CheckerApp extends spark.Registrar(
  cls[Header],
  cls[ContigLengths],
  cls[Option[_]],
  cls[ByteRanges]
)
