package org.hammerlab.bam.check

import org.apache.spark.rdd.RDD
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ PosIterator, SeekableUncompressedBytes }
import org.hammerlab.hadoop.Configuration
import org.hammerlab.io.CachingChannel._
import org.hammerlab.io.SeekableByteChannel
import org.hammerlab.iterator.FinishingIterator._
import org.hammerlab.paths.Path
import org.hammerlab.spark.Context

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
  PosResult: ClassTag
]
  extends Serializable {

  type Calls = RDD[(Pos, Call)]

  /**
   * Given a bgzf-decompressed byte stream and map from reference indices to lengths, build a [[Checker]]
   */
  def makeChecker(path: Path,
                  contigLengths: ContigLengths): Checker[Call]

  // Configurable logic for building a [[PosResult]] from a [[Call]]
  def makePosResult: MakePosResult[Call, PosResult]

  /**
   * Main CLI entry point: build a [[Result]] and print some statistics about it.
   */
  def getCalls(args: Args)(implicit sc: Context, path: Path): (Calls, Blocks) = {
    implicit val conf: Configuration = sc.hadoopConfiguration

    val Header(contigLengths, _, _) = Header(path)

    val blocks = Blocks(args)

    /**
     * Apply a [[PosCallIterator]] to each block, generating [[Call]]s.
     */
    val calls: Calls =
      blocks
        .partitionedBlocks
        .mapPartitions {
          blocks ⇒
            val checker =
              makeChecker(
                path,
                contigLengths
              )

            blocks
              .flatMap(PosIterator(_))
              .map(
                pos ⇒
                  pos →
                    checker(pos)
              )
              .finish(checker.close())
        }

    (calls, blocks)
  }

  def apply(args: Args)(implicit sc: Context, path: Path): Result

  def getTrueReadPositions(args: Args,
                           blocks: Blocks)(
      implicit
      sc: Context,
      path: Path
  ): RDD[Pos] = {

    /** File with true read-record-boundary positions as output by [[org.hammerlab.bam.index.IndexRecords]]. */
    val recordsFile: Path =
      args
        .records
        .getOrElse(
          path + ".records"
        )

//    val whitelistBroadcast = blocks.whitelistBroadcast

    /** Parse the true read-record-boundary positions from [[recordsFile]] */
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
        case Pos(blockPos, _) ⇒
          blocks.whitelist
            .forall(_.blocks(blockPos))
      }
      .cache
  }
}

trait UncompressedStreamRun[
  Call,
  PosResult
] {
  self: Run[Call, PosResult] ⇒

  def makeChecker: (SeekableUncompressedBytes, ContigLengths) ⇒ Checker[Call]

  override def makeChecker(path: Path,
                           contigLengths: ContigLengths): Checker[Call] = {
    val channel = SeekableByteChannel(path).cache
    val stream = SeekableUncompressedBytes(channel)
    makeChecker(stream, contigLengths)
  }
}
