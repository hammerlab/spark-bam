package org.hammerlab.bam.check

import java.net.URI

import caseapp._
import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.hammerlab.bam.check.Error.{ Counts, CountsWrapper, Flags, toCounts }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ Metadata, SeekableByteStream }
import org.hammerlab.bgzf.hadoop.RecordReader.MetadataReader
import org.hammerlab.bgzf.hadoop.{ BlocksSplit, IndexedInputFormat }
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.hadoop.Path
import org.hammerlab.magic.rdd.hadoop.HadoopRDD._
import org.hammerlab.magic.rdd.hadoop.SerializableConfiguration
import org.hammerlab.math.Monoid.{ mzero ⇒ zero }
import org.hammerlab.math.MonoidSyntax._
import org.seqdoop.hadoop_bam.util.SAMHeaderReader.readSAMHeaderFrom
import org.seqdoop.hadoop_bam.util.WrapSeekable

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.reflect.ClassTag

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
    val confBroadcast = sc.broadcast(new SerializableConfiguration(conf))
    val path = Path(new URI(args.bamFile))

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

    val fmt =
      IndexedInputFormat(
        path,
        conf,
        args.blocksFile,
        args.blocksPerPartition,
        args.numBlocks,
        args
          .blocksWhitelist
          .map(
            _
              .split(",")
              .map(_.toLong)
              .toSet
          )
      )

    val blocks: RDD[(Long, Metadata)] =
      sc
        .loadHadoopRDD(
          path,
          fmt.splits
        )

    val recordsFile =
      args
        .recordsFile
        .getOrElse(
          args.bamFile + ".records"
        )

    val blocksWhitelist =
      sc.broadcast(
        fmt
          .splits
          .flatMap(_.blocks)
          .map(_.start)
          .toSet
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
          blocksWhitelist.value(pos.blockPos)
      }

    val errors: RDD[(Pos, Option[Flags])] =
      blocks
        .flatMap {
          case (blockPos, Metadata(_, _, uncompressedSize)) ⇒
            val stream =
              SeekableByteStream(
                path
                .getFileSystem(confBroadcast)
                .open(path)
              )

            logger.info(s"Processing block: $blockPos")
            PosCallIterator(
              blockPos,
              uncompressedSize,
              stream,
              contigLengths
            )
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
      countsByNonZeroFields
    )
  }
}
