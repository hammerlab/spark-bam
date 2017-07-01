package org.hammerlab.bam.check

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ Metadata, SeekableUncompressedBytes }
import org.hammerlab.hadoop.Configuration
import org.hammerlab.io.CachingChannel._
import org.hammerlab.io.{ SampleSize, SeekableByteChannel }
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.magic.rdd.partitions.OrderedRepartitionRDD._
import org.hammerlab.magic.rdd.partitions.PartitionByKeyRDD._
import org.hammerlab.magic.rdd.size._
import org.hammerlab.math.ceil
import org.hammerlab.paths.Path
import org.hammerlab.spark.Context

import scala.math.min
import scala.reflect.ClassTag

/**
 * Interface for applying a [[Checker]] to a BAM file and collecting+printing statistics about its accuracy in
 * identifying read-start positions.
 *
 * @tparam Call per-position output of [[Checker]]
 * @tparam PosResult result of "scoring" a [[Call]] at a given position (i.e. identifying whether it was a [[True]] or
 *                   [[False]] call)
 */
abstract class Run[
  Call: ClassTag,
  PosResult: ClassTag,
  Res <: Result[PosResult]
]
  extends Serializable {

  /**
   * Given a bgzf-decompressed byte stream and map from reference indices to lengths, build a [[Checker]]
   */
  def makeChecker(path: Path,
                  contigLengths: ContigLengths)(
      implicit conf: Configuration
  ): Checker[Call]


  // Configurable logic for building a [[PosResult]] from a [[Call]]
  def makePosResult: MakePosResult[Call, PosResult]

  /**
   * Main CLI entry point: build a [[Result]] and print some statistics about it.
   */
  def getCalls(args: Args)(implicit sc: Context, path: Path): (RDD[(Pos, Call)], Option[Set[Long]]) = {
    implicit val conf: Configuration = sc.hadoopConfiguration
    val confBroadcast = sc.broadcast(conf)

    val Header(contigLengths, _, _) = Header(path)

    val blocksPath: Path =
      args
        .blocksFile
        .getOrElse(
          path + ".blocks"
        )

    /** Parse BGZF-block [[Metadata]] emitted by [[org.hammerlab.bgzf.index.IndexBlocks]] */
    val allBlocks =
      sc
        .textFile(blocksPath.toString)
        .map(
          line ⇒
            line.split(",") match {
              case Array(block, compressedSize, uncompressedSize) ⇒
                Metadata(
                  block.toLong,
                  compressedSize.toInt,
                  uncompressedSize.toInt
                )
              case _ ⇒
                throw new IllegalArgumentException(
                  s"Bad blocks-index line: $line"
                )
            }
        )

    val blocksWhitelist =
      args
        .blocksWhitelist
        .map(
          _
            .split(",")
            .map(_.toLong)
            .toSet
        )

    /**
     * Apply any applicable filters to [[allBlocks]]; also store the set of filtered blocks, if applicable.
     */
    val (blocks, filteredBlockSet) =
      (blocksWhitelist, args.numBlocks) match {
        case (Some(_), Some(_)) ⇒
          throw new IllegalArgumentException(
            s"Specify exactly one of {blocksWhitelist, numBlocks}"
          )
        case (Some(whitelist), _) ⇒
          allBlocks
            .filter {
              case Metadata(block, _, _) ⇒
                whitelist.contains(block)
            } →
            Some(whitelist)
        case (_, Some(numBlocks)) ⇒
          val filteredBlocks = allBlocks.take(numBlocks)
          sc.parallelize(filteredBlocks) →
            Some(
              filteredBlocks
                .map(_.start)
                .toSet
            )
        case _ ⇒
          allBlocks → None
      }

    val numBlocks = blocks.size

    val blocksPerPartition = args.blocksPerPartition

    val numPartitions =
      ceil(
        numBlocks,
        blocksPerPartition.toLong
      )
      .toInt

    /** Repartition [[blocks]] to obey [[blocksPerPartition]] constraint. */
    val partitionedBlocks =
      (for {
        (block, idx) ← blocks.zipWithIndex()
      } yield
        (idx / blocksPerPartition).toInt →
          idx →
            block
      )
      .partitionByKey(numPartitions)

    /**
     * Apply a [[PosCallIterator]] to each block, generating [[Call]]s.
     */
    val calls: RDD[(Pos, Call)] =
      partitionedBlocks
        .mapPartitions {
          blocks ⇒
            val checker =
              makeChecker(
                path,
                contigLengths
              )(
                confBroadcast.value
              )

            blocks
              .flatMap {
                case Metadata(start, _, uncompressedSize) ⇒
                  PosCallIterator(
                    start,
                    uncompressedSize,
                    checker
                  )
              }
              .finish(checker.close())
        }

    calls → filteredBlockSet
  }

  def apply(args: Args)(implicit sc: Context, path: Path): Res = {
    val (calls, filteredBlockSet) = getCalls(args)

    /** File with true read-record-boundary positions as output by [[org.hammerlab.bam.index.IndexRecords]]. */
    val recordsFile: Path =
      args
        .recordsFile
        .getOrElse(
          path + ".records"
        )

    /** Parse the true read-record-boundary positions from [[recordsFile]] */
    val recordPosRDD: RDD[Pos] =
      sc
        .textFile(recordsFile.toString)
        .map(
          line ⇒
            line.split(",") match {
              case Array(a, b) ⇒
                Pos(a.toLong, b.toInt)
              case _ ⇒
                throw new IllegalArgumentException(
                  s"Bad record-pos line: $line"
                )
            }
        )
        .filter {
          pos ⇒
            filteredBlockSet
              .forall(_ (pos.blockPos))
        }

    /**
     * Join per-[[Pos]] [[Call]]s against the set of true read-record boundaries, [[recordPosRDD]], making a
     * [[PosResult]] for each that records {[[True]],[[False]]} x {[[Positive]],[[Negative]]} information as well
     * as optional info from the [[Call]].
     */
    val results: RDD[(Pos, PosResult)] =
      calls
        .fullOuterJoin(
          recordPosRDD.map(
            _ → null
          )
        )
        .map {
          case (pos, (callOpt, isReadStart)) ⇒
            pos →
              callOpt
                .map {
                  call ⇒
                    makePosResult(
                      call,
                      isReadStart.isDefined
                    )
                }
                .getOrElse(
                  throw new Exception(s"No call detected at $pos")
                )
        }
        .sortByKey()

    val posResultPartitioner =
      new Partitioner {
        override def numPartitions: Int = 4
        override def getPartition(key: Any): Int =
          key.asInstanceOf[check.PosResult] match {
            case _:  TruePositive ⇒ 0
            case _:  TrueNegative ⇒ 1
            case _: FalsePositive ⇒ 2
            case _: FalseNegative ⇒ 3
          }
      }

    /** Compute {[[True]],[[False]]} x {[[Positive]],[[Negative]]} counts in one stage */
    val trueFalseCounts: Map[check.PosResult, Long] =
      results
        .values
        .map {
          case _:  TruePositive ⇒  TruePositive → 1L
          case _:  TrueNegative ⇒  TrueNegative → 1L
          case _: FalsePositive ⇒ FalsePositive → 1L
          case _: FalseNegative ⇒ FalseNegative → 1L
        }
        .reduceByKey(
          posResultPartitioner,
          _ + _
        )
        .collectAsMap
        .toMap

    val numCalls = trueFalseCounts.values.sum

    val numCalledReadStarts =
      trueFalseCounts
        .filterKeys {
          case _: Positive ⇒ true
          case _: Negative ⇒ false
        }
        .values
        .sum

    val numFalseCalls =
      trueFalseCounts
        .filterKeys {
          case _: False ⇒ true
          case _: True ⇒ false
        }
        .values
        .sum

    val falseCalls =
      results
        .flatMap {
          case (pos, f: False) ⇒
            Some(pos → (f: False))
          case _ ⇒
            None
        }
        .orderedRepartition(
          min(
            results.getNumPartitions,
            ceil(
              numFalseCalls,
              args.resultPositionsPerPartition
            )
            .toInt
          )
        )

    val calledReadStarts =
      results
        .flatMap {
          case (pos, _: Positive) ⇒
            Some(pos)
          case _ ⇒
            None
        }
        .orderedRepartition(
          min(
            results.getNumPartitions,
            ceil(
              numCalledReadStarts,
              args.resultPositionsPerPartition
            )
            .toInt
          )
        )

    implicit val sampleSize = args.samplesToPrint

    makeResult(
      numCalls,
      results,
      numFalseCalls,
      falseCalls,
      numCalledReadStarts,
      calledReadStarts
    )
  }

  /**
   * Configurable final stage of [[Result]]-building
   */
  def makeResult(numCalls: Long,
                 results: RDD[(Pos, PosResult)],
                 numFalseCalls: Long,
                 falseCalls: RDD[(Pos, False)],
                 numCalledReadStarts: Long,
                 calledReadStarts: RDD[Pos])(implicit sampleSize: SampleSize): Res
}

trait UncompressedStreamRun[
  Call,
  PosResult,
  Result <: check.Result[PosResult]
] {
  self: Run[Call, PosResult, Result] ⇒

  def makeChecker: (SeekableUncompressedBytes, ContigLengths) ⇒ Checker[Call]

  override def makeChecker(path: Path,
                           contigLengths: ContigLengths)(
      implicit conf: Configuration
  ): Checker[Call] = {
    val channel = SeekableByteChannel(path).cache
    val stream = SeekableUncompressedBytes(channel)
    makeChecker(stream, contigLengths)
  }
}
