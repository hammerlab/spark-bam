package org.hammerlab.bam.check

import java.net.URI

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ Metadata, SeekableByteStream }
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.hadoop.Path
import org.hammerlab.magic.rdd.hadoop.SerializableConfiguration
import org.hammerlab.magic.rdd.partitions.PartitionByKeyRDD._
import org.hammerlab.magic.rdd.size._
import org.seqdoop.hadoop_bam.util.SAMHeaderReader.readSAMHeaderFrom
import org.seqdoop.hadoop_bam.util.WrapSeekable

import scala.collection.JavaConverters._
import scala.math.ceil
import scala.reflect.ClassTag

abstract class Run[Call: ClassTag, PosResult: ClassTag]
 extends Serializable {

  def makeChecker: (SeekableByteStream, Map[Int, NumLoci]) ⇒ Checker[Call]

  def apply(sc: SparkContext, args: Args): Result[PosResult] = {
    val result = getResult(sc, args)

    import Result.sampleString

    val Result(
      numCalls,
      _,
      numFalseCalls,
      _
    ) =
      result

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
            result
              .falseCallsHistSampleSize
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

    result
  }

  def makePosResult: MakePosResult[Call, PosResult]

  def getResult(sc: SparkContext, args: Args): Result[PosResult] = {
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

    val calls: RDD[(Pos, Call)] =
      partitionedBlocks
        .flatMap {
          case Metadata(start, _, uncompressedSize) ⇒

            val is =
              path
              .getFileSystem(confBroadcast)
              .open(path)

            val stream = SeekableByteStream(is)

            new PosCallIterator(
              start,
              uncompressedSize,
              makeChecker(stream, contigLengths)
            ) {
              override def done(): Unit = {
                super.done()
                is.close()
              }
            }
        }

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

    // Compute true/false/total counts in one stage
    val trueFalseCounts =
      results
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
      results.flatMap {
        case (pos, f: False) ⇒
          Some(pos → (f: False))
        case _ ⇒
          None
      }

    makeResult(
      numCalls,
      results,
      numFalseCalls,
      falseCalls
    )
  }

  def makeResult(numCalls: Long,
                 results: RDD[(Pos, PosResult)],
                 numFalseCalls: Long,
                 falseCalls: RDD[(Pos, False)]): Result[PosResult]
}
