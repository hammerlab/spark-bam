package org.hammerlab.bam.check

import java.io.ByteArrayInputStream

import caseapp._
import grizzled.slf4j.Logging
import org.apache.hadoop.fs.{ Path ⇒ HPath }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.hammerlab.bam.check.Error.{ Counts, CountsWrapper, Flags, toCounts }
import org.hammerlab.bgzf.hadoop.CompressedBlocksInputFormat.RANGES_KEY
import org.hammerlab.bgzf.hadoop.{ CompressedBlock, CompressedBlocksInputFormat }
import org.hammerlab.bgzf.{ Pos, block }
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.magic.rdd.keyed.FilterKeysRDD._
import org.hammerlab.magic.rdd.keyed.ReduceByKeyRDD._
import org.hammerlab.magic.rdd.sliding.SlidingRDD._
import org.hammerlab.magic.rdd.sort.SortRDD._
import org.hammerlab.math.Monoid.{ mzero ⇒ zero }
import org.hammerlab.math.MonoidSyntax._
import org.seqdoop.hadoop_bam.util.SAMHeaderReader.readSAMHeaderFrom
import org.seqdoop.hadoop_bam.util.WrapSeekable

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.math.ceil
import scala.reflect.ClassTag

case class Args(@ExtraName("b") bamFile: String,
                @ExtraName("k") blocksFile: Option[String] = None,
                @ExtraName("r") recordsFile: Option[String] = None,
                @ExtraName("n") numBlocks: Option[Int] = None,
                @ExtraName("w") blocksWhitelist: Option[String] = None,
                @ExtraName("p") blocksPerPartition: Int = 20,
                @ExtraName("f") blocksToBuffer: Int = 3)

sealed trait Call

sealed trait True extends Call
sealed trait False extends Call

case object TruePositive extends True
case object FalsePositive extends False
case class TrueNegative(error: Flags) extends True
case class FalseNegative(error: Flags) extends False

/**
 * Statistics about [[Guesser]]'s performance identifying read-record-boundaries in a BAM file.
 *
 * @param numCalls number of [[Pos]]s evaluated; this is the block-decompressed size of the input BAM file
 * @param calls [[RDD]] of [[Call]]s, keyed by [[Pos]]
 * @param numFalseCalls number of "false [[Call]]s": read-boundaries that were ruled out by [[Guesser]]
 *                      ([[FalseNegative]]s) and read-boundaries predicted by [[Guesser]] that aren't actually
 *                      read-record-boundaries in the input BAM ([[FalsePositive]]s)
 * @param falseCalls [[RDD]] of [[False]] [[Call]]s
 * @param criticalErrorCounts how many times each flag was the *only* flag identifying a [[TrueNegative]]
 *                            read-boundary-candidate as negative
 * @param totalErrorCounts how many times each flag identified a [[TrueNegative]] read-boundary-candidate as negative
 * @param countsByNonZeroFields for each `n`:
 *                              - the number of times each flag was set at [[Pos]]s with exactly `n` flags set, as well
 *                                as…
 *                              - the number of times it was set for [[Pos]]s with `≤ n` flags set
 */
case class Result(numCalls: Long,
                  calls: RDD[(Pos, Call)],
                  numFalseCalls: Long,
                  falseCalls: RDD[(Pos, False)],
                  criticalErrorCounts: Counts,
                  totalErrorCounts: Counts,
                  countsByNonZeroFields: Array[(Int, (Counts, Counts))])

object Main
  extends CaseApp[Args]
    with Logging {

  implicit def CanBuildArrayFromSeq[T, U: ClassTag] =
    new CanBuildFrom[Seq[T], U, Array[U]] {
      def apply(from: Seq[T]): mutable.Builder[U, Array[U]] = mutable.ArrayBuilder.make[U]()
      def apply(): mutable.Builder[U, Array[U]] = mutable.ArrayBuilder.make[U]()
    }

  /**
   * Entry-point delegated to by [[caseapp]]'s [[main]]; computes [[Result]] and prints some statistics to stdout.
   */
  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {

    val sparkConf = new SparkConf()

    val sc = new SparkContext(sparkConf)

    val Result(
      numCalls,
      _,
      numFalseCalls,
      falseCalls,
      criticalErrorCounts,
      totalErrorCounts,
      _
    ) =
      run(sc, args)

    val falseCallsSample =
      if (numFalseCalls > 1000)
        falseCalls.take(1000)
      else
        falseCalls.collect()

    numFalseCalls match {
      case 0 ⇒
        println(s"$numCalls calls, no errors!")
      case _ ⇒
        println(s"$numCalls calls, $falseCallsSample errors; first 1000:")
        println(
          falseCallsSample
            .mkString(
              "\t",
              "\n\t",
              if (numFalseCalls > 1000)
                "\n…\n"
              else
                "\n"
            )
        )
    }

    println("Critical error counts (true negatives where only one check failed):")
    println(criticalErrorCounts.pp(includeZeros = false))
    println("")
    println("Total error counts:")
    println(totalErrorCounts.pp())
    println("")
  }

  /**
   * [[run]]/[[main]] overload with a more-structured [[Result]] type for further ad-hoc/downstream inspection.
   */
  def run(sc: SparkContext, args: Args): Result = {

    val conf = sc.hadoopConfiguration
    val path = new HPath(args.bamFile)

    val ss = WrapSeekable.openPath(conf, path)

    /**
     * [[htsjdk.samtools.SAMFileHeader]] information used in identifying valid reads: contigs by index and their lengths
     */
    val contigLengths: Map[Int, NumLoci] =
      readSAMHeaderFrom(ss, conf)
        .getSequenceDictionary
        .getSequences
        .asScala
        .map(
          seq ⇒
            seq.getSequenceIndex →
              NumLoci(seq.getSequenceLength)
        )
        .toMap

    val fs = path.getFileSystem(conf)

    /**
     * BGZF (and hence BAM) files end up with an empty 28-byte bgzf block which is not output by
     * [[org.hammerlab.bgzf.IndexBlocks]].
     */
    val lastBlock = fs.getFileStatus(path).getLen - 28

    val blocksFile =
      args
        .blocksFile
        .getOrElse(
          args.bamFile + ".blocks"
        )

    /** Parse block-positions and uncompressed-sizes from [[blocksFile]] */
    val blockSizes: RDD[(Long, Int)] =
      sc
        .textFile(blocksFile)
        .map(
          _.split(",") match {
            case Array(pos, usize) ⇒
              pos.toLong → usize.toInt
            case a ⇒
              throw new IllegalArgumentException(s"Bad block line: ${a.mkString(",")}")
          }
        )

    /** Pair each block with its successor */
    val blocksAndSuccessors =
      blockSizes
        .keys
        .sliding2Opt
        .mapValues(_.getOrElse(lastBlock))

    /**
     * If [[args.numBlocks]] or [[args.blocksWhitelist]] is set, build a [[Broadcast]]/[[Set]] with the eligible
     * block-positions.
     */
    val blockWhitelistOpt: Option[Broadcast[Set[Long]]] =
      (args.numBlocks, args.blocksWhitelist) match {
        case (Some(_), Some(_)) ⇒
          throw new Exception("Specify num blocks xor blocks whitelist")
        case (Some(numBlocks), _) ⇒
          Some(
            sc.broadcast(
              blockSizes
                .keys
                .take(numBlocks)
                .toSet
            )
          )
        case (_, Some(blocksWhitelist)) ⇒
          Some(
            sc.broadcast(
              blocksWhitelist
                .split(",")
                .map(_.toLong)
                .toSet
            )
          )
        case _ ⇒
          None
      }

    /**
     * Helper for restricting block-pos-keyed [[RDD]]s to the above-computed [[blockWhitelistOpt]]
     */
    def filterBlocks[T: ClassTag](rdd: RDD[(Long, T)]): RDD[(Long, T)] =
      blockWhitelistOpt match {
        case Some(blockWhitelist) ⇒
          rdd.filterKeys(blockWhitelist)
        case None ⇒
          rdd
      }

    val recordsFile =
      args
        .recordsFile
        .getOrElse(
          args.bamFile + ".records"
        )

    /** Parse the true read-record-boundary positions from [[recordsFile]] */
    val recordsRDD: RDD[(Long, Int)] =
      sc
        .textFile(recordsFile)
        .map(
          _.split(",") match {
            case Array(a, b) ⇒
              (a.toLong, b.toInt)
            case a ⇒
              throw new IllegalArgumentException(s"Bad record-pos line: ${a.mkString(",")}")
          }
        )

    /**
     * Pair each block with the first valid read-[[Pos]] from the block that follows it.
     *
     * This is useful because when testing all positions in a block, valid reads beginning toward the end of the block
     * may need access to all bytes up to the next true read-start in order to validate their current position.
     */
    val blockNextFirstOffsets: RDD[(Long, Option[Pos])] =
      recordsRDD
        .minByKey()
        .sortByKey()
        .sliding2Opt
        .map {
          case ((block, _), nextBlockOpt) ⇒
            block →
              nextBlockOpt
                .map {
                  case (nextBlock, nextOffset) ⇒
                    Pos(nextBlock, nextOffset)
                }
        }

    /** Pair each block with the read-offsets it contains */
    val blockOffsetsRDD: RDD[(Long, Vector[Int])] =
      recordsRDD
        .groupByKey()
        .mapValues(
          _
            .toVector
            .sorted
        )

    val blockDataRDD: RDD[(Long, (Int, Vector[Int], Option[Pos]))] =
      filterBlocks(blockSizes)
        .cogroup(
          filterBlocks(blockOffsetsRDD),
          filterBlocks(blockNextFirstOffsets)
        )
        .flatMap {
          case (block, (usizes, offsetss, nextPosOpts)) ⇒
            (usizes.size, offsetss.size, nextPosOpts.size) match {
              case (1, 1, 1) ⇒
                Some(
                  block →
                    (
                      usizes.head,
                      offsetss.head,
                      nextPosOpts.head
                    )
                )
              case _ ⇒
                logger.warn(
                  s"Suspicious join sizes at $block: ${usizes.size} ${offsetss.size} ${nextPosOpts.size}"
                )
                None
            }
        }

    val blocksRefrenced =
      blockDataRDD
        .flatMap {
          case (block, (_, _, nextPosOpt)) ⇒
            Iterator(block) ++ nextPosOpt.map(_.blockPos)
        }
        .distinct
        .sort()

    val referenceBlocksWithEnds =
      blocksRefrenced
        .keyBy(x ⇒ x)
        .join(blocksAndSuccessors)
        .values
        .flatMap(x ⇒ Iterator(x._1, x._2))
        .distinct
        .sort()
        .collect

    val numPartitions =
      ceil(
        referenceBlocksWithEnds.length * 1.0 / args.blocksPerPartition
      )
      .toInt

    val blocksStr = referenceBlocksWithEnds.mkString(",")

    conf.set(RANGES_KEY, blocksStr)

    val blockBytesRDD: RDD[(Long, Array[Byte])] =
      sc
        .newAPIHadoopFile(
          args.bamFile,
          classOf[CompressedBlocksInputFormat],
          classOf[Long],
          classOf[CompressedBlock]
        )
        .sliding(args.blocksToBuffer + 1, includePartial = true)  // Get each BGZF block and 3 that follow it
        .map {
          bytess ⇒
            bytess
            .head
            ._1 →
              block.Stream(
                new ByteArrayInputStream(
                  bytess
                    .flatMap(_._2.bytes)
                )
              )
              .flatMap(_.bytes)
              .toArray
        }

    val calls =
      blockDataRDD
        .leftOuterJoin(blockBytesRDD, numPartitions)
        .flatMap {
          case (
            block,
            (
              (
                usize,
                offsets,
                nextPosOpt
              ),
              Some(bytes)
            )
          ) ⇒
            PosCallIterator(
              block,
              usize,
              offsets,
              nextPosOpt,
              bytes,
              contigLengths
            )
          case (block, (dataOpt, bytesOpt)) ⇒
            throw new Exception(
              s"Missing data or bytes for block $block: $dataOpt $bytesOpt"
            )
        }
        .sortByKey()

    // Compute true/false/total counts in one stage
    val trueFalseCounts =
      calls
        .values
        .map {
          case _: False ⇒ false → 1L
          case _: True ⇒ true → 1L
        }
        .reduceByKey(_ + _, 2)
        .collectAsMap
        .toMap

    val numCalls = trueFalseCounts.values.sum
    val numFalseCalls = trueFalseCounts(false)

    val falseCalls =
      calls.flatMap {
        case (pos, f: False) ⇒ Some(pos → f)
        case _ ⇒ None
      }

    /**
     * How many times each flag correctly rules out a [[Pos]], grouped by how many total flags ruled out that [[Pos]].
     *
     * Useful for identifying e.g. flags that tend to be "critical" (necessary to avoid [[FalsePositive]] read-boundary
     * [[Call]]s).
     */
    val trueNegativesByNumNonzeroFields: Array[(Int, Counts)] =
      calls
        .flatMap {
          _._2 match {
            case TrueNegative(error) ⇒ Some(toCounts(error))
            case _ ⇒ None
          }
        }
        .keyBy(_.numNonZeroFields)
        .reduceByKey(_ |+| _, 20)  // Total number of distinct keys will be the number of fields in an [[ErrorT]]
        .collect()
        .sortBy(_._1)

    /**
     * CDF to [[trueNegativesByNumNonzeroFields]]'s PDF: how many times does each flag correctly rule out [[Pos]]s that
     * were ruled out by *at most `n`* total flags, for each `n`.
     */
    val trueNegativesByNumNonzeroFieldsCumulative: Array[(Int, Counts)] =
      trueNegativesByNumNonzeroFields
        .scanLeft(
          0 → zero[Counts]
        ) {
          (soFar, next) ⇒
            val (_, countSoFar) = soFar
            val (numNonZeroFields, count) = next
            numNonZeroFields → (countSoFar |+| count)
        }
        .drop(1)  // Discard the dummy/initial "0" entry added above to conform to [[scanLeft]] API

    /**
     * Zip [[trueNegativesByNumNonzeroFields]] and [[trueNegativesByNumNonzeroFieldsCumulative]]: PDF and CDF, keyed by
     * the number of flags ruling out positions.
     */
    val countsByNonZeroFields: Array[(Int, (Counts, Counts))] =
      for {
        ((numNonZeroFields, counts), (_, cumulativeCounts)) ←
          trueNegativesByNumNonzeroFields
            .zip(
              trueNegativesByNumNonzeroFieldsCumulative
            )
      } yield
        numNonZeroFields → (counts, cumulativeCounts)

    /**
     * "Critical" error counts: how many times each flag was the *only* flag identifying a read-boundary-candidate as
     * false.
     */
    val criticalErrorCounts = countsByNonZeroFields.head._2._1

    /**
     * "Total" error counts: how many times each flag ruled out a position, over the entire dataset
     */
    val totalErrorCounts = countsByNonZeroFields.last._2._2

    Result(
      numCalls,
      calls,
      numFalseCalls,
      falseCalls,
      criticalErrorCounts,
      totalErrorCounts,
      countsByNonZeroFields
    )
  }
}
