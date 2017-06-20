package org.hammerlab.bam.check

import java.net.URI

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.Result.sampleString
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ Metadata, SeekableUncompressedBytes }
import org.hammerlab.hadoop.{ Path, SerializableConfiguration }
import org.hammerlab.io.SeekableByteChannel.SeekableHadoopByteChannel
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.magic.rdd.partitions.PartitionByKeyRDD._
import org.hammerlab.magic.rdd.size._

import scala.math.ceil
import scala.reflect.ClassTag

/**
 * Interface for applying a [[Checker]] to a BAM file and collecting+printing statistics about its accuracy in
 * identifying read-start positions.
 *
 * @tparam Call per-position output of [[Checker]]
 * @tparam PosResult result of "scoring" a [[Call]] at a given position (i.e. identifying whether it was a [[True]] or
 *                   [[False]] call)
 */
abstract class Run[Call: ClassTag, PosResult: ClassTag]
 extends Serializable {

  /**
   * Given a bgzf-decompressed byte stream and map from reference indices to lengths, build a [[Checker]]
   */
  def makeChecker: (SeekableUncompressedBytes, ContigLengths) ⇒ Checker[Call]

  /**
   * Main CLI entry point: build a [[Result]] and print some statistics about it.
   */
  def apply(sc: SparkContext, args: Args): Result[PosResult] = {

    val result = getResult(sc, args)

    val Result(
      numCalls,
      numFalseCalls,
      numReadStarts
    ) =
      result

    numFalseCalls match {
      case 0 ⇒
        println(s"$numCalls calls ($numReadStarts reads), no errors!")
      case _ ⇒

        println(s"$numCalls calls ($numReadStarts reads), $numFalseCalls errors")

        println(s"False-call histogram:")
        println(
          sampleString(
            result
              .falseCallsHistSample
              .map {
                case (count, call) ⇒
                  s"$count:\t$call"
              },
            result
              .falseCallsHistSampleSize
          )
        )
        println("")

        println(s"First ${result.falseCallsSampleSize} false calls:")
        println(
          sampleString(
            result
              .falseCallsSample
              .map {
                case (pos, call) ⇒
                  s"$pos:\t$call"
              },
            numFalseCalls
          )
        )
        println("")
    }

    result
  }

  // Configurable logic for building a [[PosResult]] from a [[Call]]
  def makePosResult: MakePosResult[Call, PosResult]

  def getResult(sc: SparkContext, args: Args): Result[PosResult] = {
    val conf = sc.hadoopConfiguration
    val confBroadcast = sc.broadcast(new SerializableConfiguration(conf))
    val path = Path(new URI(args.bamFile))

    val Header(contigLengths, _, _) = Header(path, sc.hadoopConfiguration)

    val blocksPath =
      args
        .blocksFile
        .map(str ⇒ Path(new URI(str)))
        .getOrElse(path.suffix(".blocks"): Path)

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
              throw new IllegalArgumentException(s"Bad blocks-index line: $line")
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
        numBlocks * 1.0 / blocksPerPartition
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

    /** File with true read-record-boundary positions as output by [[org.hammerlab.bam.index.IndexRecords]]. */
    val recordsFile =
      args
        .recordsFile
        .getOrElse(
          args.bamFile + ".records"
        )

    /** Parse the true read-record-boundary positions from [[recordsFile]] */
    val recordPosRDD: RDD[Pos] =
      sc
        .textFile(recordsFile)
        .map(
          _.split(",") match {
            case Array(a, b) ⇒
              Pos(a.toLong, b.toInt)
            case a ⇒
              throw new IllegalArgumentException(s"Bad record-pos line: ${a.mkString(",")}")
          }
        )
        .filter {
          pos ⇒
            filteredBlockSet
            .forall(_ (pos.blockPos))
        }

    /**
     * Apply a [[PosCallIterator]] to each block, generating [[Call]]s.
     */
    val calls: RDD[(Pos, Call)] =
      partitionedBlocks
        .flatMap {
          case Metadata(start, _, uncompressedSize) ⇒

            val channel = SeekableHadoopByteChannel(path, confBroadcast.value)
            val stream = SeekableUncompressedBytes(channel)

            PosCallIterator(
              start,
              uncompressedSize,
              makeChecker(stream, contigLengths)
            )
            .finish(
              stream.close()
            )
        }

    /**
     * Join per-[[Pos]] [[Call]]s against the set of true read-record boundaries, [[recordPosRDD]], making a
     * [[PosResult]] for each that records {[[True]],[[False]]} x {[[Positive]],[[Negative]]} information as well
     * as optional info from the [[Call]].
     */
    val results: RDD[(Pos, PosResult)] =
      calls
        .fullOuterJoin(recordPosRDD.map(_ → null))
        .map {
          case (pos, (callOpt, isReadStart)) ⇒
            pos → (
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
            )
        }

    /** Compute {[[True]],[[False]]} x {[[Positive]],[[Negative]]} counts in one stage */
    val trueFalseCounts: Map[check.PosResult, Long] =
      results
        .values
        .map {
          case _: TruePositive ⇒ TruePositive → 1L
          case _: TrueNegative ⇒ TrueNegative → 1L
          case _: FalsePositive ⇒ FalsePositive → 1L
          case _: FalseNegative ⇒ FalseNegative → 1L
        }
        .reduceByKey(_ + _, 4)
        .collectAsMap
        .toMap

    val numCalls = trueFalseCounts.values.sum

    val numReadStarts =
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
      results.flatMap {
        case (pos, f: False) ⇒
          Some(pos → (f: False))
        case _ ⇒
          None
      }

    val readStarts =
      results
        .flatMap {
          case (pos, _: Positive) ⇒
            Some(pos)
          case _ ⇒
            None
        }

    makeResult(
      numCalls,
      results,
      numFalseCalls,
      falseCalls,
      numReadStarts,
      readStarts
    )
  }

  /**
   * Configurable final stage of [[Result]]-building
   */
  def makeResult(numCalls: Long,
                 results: RDD[(Pos, PosResult)],
                 numFalseCalls: Long,
                 falseCalls: RDD[(Pos, False)],
                 numReadStarts: Long,
                 readStarts: RDD[Pos]): Result[PosResult]
}
