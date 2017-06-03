package org.hammerlab.bam.check

import java.net.URI

import caseapp._
import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.hammerlab.bam.check.Error.{ Counts, CountsWrapper, Flags, toCounts }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ Metadata, SeekableByteStream }
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.hadoop.Path
import org.hammerlab.magic.rdd.hadoop.SerializableConfiguration
import org.hammerlab.magic.rdd.partitions.PartitionByKeyRDD._
import org.hammerlab.magic.rdd.size._
import org.hammerlab.math.Monoid.{ mzero ⇒ zero }
import org.hammerlab.math.MonoidSyntax._
import org.seqdoop.hadoop_bam.util.SAMHeaderReader.readSAMHeaderFrom
import org.seqdoop.hadoop_bam.util.WrapSeekable

import scala.collection.JavaConverters._
import scala.collection.SortedMap
import scala.math.ceil

case class Args(@ExtraName("b") bamFile: String,
                @ExtraName("k") blocksFile: Option[String] = None,
                @ExtraName("r") recordsFile: Option[String] = None,
                @ExtraName("n") numBlocks: Option[Int] = None,
                @ExtraName("w") blocksWhitelist: Option[String] = None,
                @ExtraName("p") blocksPerPartition: Int = 20)

sealed trait Call

sealed trait True extends Call
sealed trait False extends Call

case object TruePositive extends True
case object FalsePositive extends False
case class TrueNegative(error: Flags) extends True
case class FalseNegative(error: Flags) extends False

/**
 * Statistics about [[RecordFinder]]'s performance identifying read-record-boundaries in a BAM file.
 *
 * @param numCalls              number of [[Pos]]s evaluated; this is the block-decompressed size of the input BAM file
 * @param calls                 [[RDD]] of [[Call]]s, keyed by [[Pos]]
 * @param numFalseCalls         number of "false [[Call]]s": read-boundaries that were ruled out by [[RecordFinder]]
 *                              ([[FalseNegative]]s) and read-boundaries predicted by [[RecordFinder]] that aren't actually
 *                              read-record-boundaries in the input BAM ([[FalsePositive]]s)
 * @param falseCalls            [[RDD]] of [[False]] [[Call]]s
 * @param criticalErrorCounts   how many times each flag was the *only* flag identifying a [[TrueNegative]]
 *                              read-boundary-candidate as negative
 * @param totalErrorCounts      how many times each flag identified a [[TrueNegative]] read-boundary-candidate as negative
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
                  countsByNonZeroFields: SortedMap[Int, (Counts, Counts)]) {

  var falseCallsSampleSize = 100

  lazy val falseCallsSample =
    if (numFalseCalls > falseCallsSampleSize)
      falseCalls.take(falseCallsSampleSize)
    else if (numFalseCalls > 0)
      falseCalls.collect()
    else
      Array()

  lazy val falseCallsHist =
    falseCalls
      .values
      .map(_ → 1L)
      .reduceByKey(_ + _)
      .map(_.swap)
      .sortByKey(ascending = false)

  var falseCallsHistSampleSize = 100

  lazy val falseCallsHistSample =
    if (numFalseCalls > falseCallsHistSampleSize)
      falseCallsHist.take(falseCallsHistSampleSize)
    else if (numFalseCalls > 0)
      falseCallsHist.collect()
    else
      Array()
}

object Result {
  def sampleString(sampledLines: Seq[String], total: Long): String =
    sampledLines
      .mkString(
        "\t",
        "\n\t",
        if (sampledLines.size < total)
          "\n\t…"
        else
          ""
      )
}

object Main
  extends CaseApp[Args]
    with Logging {

  /**
   * Entry-point delegated to by [[caseapp]]'s [[main]]; computes [[Result]] and prints some statistics to stdout.
   */
  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {

    val sparkConf = new SparkConf()

    val sc = new SparkContext(sparkConf)

    val result = run(sc, args)

    val Result(
      numCalls,
      _,
      numFalseCalls,
      _,
      criticalErrorCounts,
      totalErrorCounts,
      countsByNonZeroFields
    ) = result

    import Result.sampleString

    numFalseCalls match {
      case 0 ⇒
        println(s"$numCalls calls, no errors!")
      case _ ⇒

        println(s"$numCalls calls, $numFalseCalls errors")

        println(s"hist:")
        println(
          sampleString(
            result
              .falseCallsHistSample
              .map {
                case (count, call) ⇒
                  s"$count:\t$call"
              },
            result.falseCallsHistSampleSize
          )
        )
        println("")

        println(s"first ${result.falseCallsSampleSize}:")
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

    println("Critical error counts (true negatives where only one check failed):")
    println(criticalErrorCounts.pp(includeZeros = false))
    println("")

    countsByNonZeroFields
      .get(2)
      .foreach {
        counts ⇒
          println("True negatives where exactly two checks failed:")
          println(
            counts
              ._1
              .pp(includeZeros = false)
          )
          println("")
      }

    println("Total error counts:")
    println(totalErrorCounts.pp())
    println("")
  }

  /**
   * [[run]]/[[main]] overload with a more-structured [[Result]] type for further ad-hoc/downstream inspection.
   */
  def run(sc: SparkContext, args: Args): Result = {

    val conf = sc.hadoopConfiguration
    val confBroadcast = sc.broadcast(new SerializableConfiguration(conf))
    val path = Path(new URI(args.bamFile))

    val ss = WrapSeekable.openPath(conf, path)

    val blocksPath =
      args
        .blocksFile
        .map(str ⇒ Path(new URI(str)))
        .getOrElse(path.suffix(".blocks"): Path)

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

    val partitionedBlocks =
      (for {
        (block, idx) ← blocks.zipWithIndex()
      } yield
        (idx / blocksPerPartition).toInt →
          idx →
          block
      )
      .partitionByKey(numPartitions)

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

    val errors: RDD[(Pos, Option[Flags])] =
      partitionedBlocks
        .flatMap {
          case Metadata(start, _, uncompressedSize) ⇒

            val is =
              path
                .getFileSystem(confBroadcast)
                .open(path)

            val stream = SeekableByteStream(is)

            logger.info(s"Processing block: $start")
            new PosCallIterator(
              start,
              uncompressedSize,
              stream,
              contigLengths
            ) {
              override def done(): Unit = {
                super.done()
                logger.info(s"Closing stream for block $start")
                is.close()
              }
            }
        }

    val calls: RDD[(Pos, Call)] =
      errors
        .fullOuterJoin(recordPosRDD.map(_ → null))
        .map {
          case (pos, (errorOpt, isReadStart)) ⇒
            pos → (
              errorOpt
                .map {
                  error ⇒
                    (error, isReadStart) match {
                      case (Some(error), Some(_)) ⇒
                        FalseNegative(error)
                      case (Some(error), None) ⇒
                        TrueNegative(error)
                      case (None, Some(_)) ⇒
                        TruePositive
                      case (None, None) ⇒
                        FalsePositive
                    }
                }
                .getOrElse(
                  throw new Exception(s"No call detected at $pos")
                )
              )
        }

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
    val numFalseCalls = trueFalseCounts.getOrElse(false, 0L)

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
      SortedMap(countsByNonZeroFields: _*)
    )
  }
}
