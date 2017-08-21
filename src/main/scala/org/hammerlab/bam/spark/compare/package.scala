package org.hammerlab.bam.spark

import org.apache.hadoop.fs
import org.apache.hadoop.mapreduce.lib.input
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths
import org.apache.hadoop.mapreduce.{ InputSplit, Job }
import org.hammerlab.bam.check.Checker.{ BGZFBlocksToCheck, MaxReadSize, ReadsToCheck }
import org.hammerlab.bam.header.Header
import org.hammerlab.bam.spark.load.Channels
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.FindBlockStart
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.{ FileSplit, FileSplits, MaxSplitSize }
import org.hammerlab.iterator.sliding.Sliding2Iterator._
import org.hammerlab.iterator.sorted.OrZipIterator._
import org.hammerlab.paths.Path
import org.hammerlab.types.{ Both, L, R }
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit }

import scala.collection.JavaConverters._

package object compare {

  case class Result(numSparkSplits: Int,
                    numHadoopSplits: Int,
                    diffs: Vector[Either[Split, Split]],
                    numSparkOnlySplits: Int,
                    numHadoopOnlySplits: Int
                   )

  def getPathResult(path: Path)(
      implicit
      conf: Configuration,
      splitSize: MaxSplitSize,
      bgzfBlocksToCheck: BGZFBlocksToCheck,
      readsToCheck: ReadsToCheck,
      maxReadSize: MaxReadSize
  ): Result = {

    val fileSplits =
      FileSplits(
        path,
        splitSize
      )

    val hadoopBamSplits = getHadoopBamSplits(path, fileSplits)
    val  sparkBamSplits =  getSparkBamSplits(path, fileSplits)

    implicit def toStart(split: Split): Pos = split.start

    val diffs =
      sparkBamSplits
        .sortedOrZip[Split, Pos](hadoopBamSplits)
        .flatMap {
          case Both(_, _) ⇒ None
          case L(ours) ⇒ Some(Left(ours))
          case R(theirs) ⇒ Some(Right(theirs))
        }
        .toVector

    val (numSparkOnlySplits, numHadoopOnlySplits) =
      diffs.foldLeft((0, 0)) {
        case ((numSpark, numHadoop), diff) ⇒
          diff match {
            case Left(_) ⇒ (numSpark + 1, numHadoop)
            case Right(_) ⇒ (numSpark, numHadoop + 1)
          }
      }

    Result(
      sparkBamSplits.length,
      hadoopBamSplits.length,
      diffs,
      numSparkOnlySplits,
      numHadoopOnlySplits
    )
  }

  def getSparkBamSplits(path: Path,
                        fileSplits: Seq[FileSplit])(
      implicit
      conf: Configuration,
      bgzfBlocksToCheck: BGZFBlocksToCheck,
      readsToCheck: ReadsToCheck,
      maxReadSize: MaxReadSize
  ): Vector[Split] = {
    implicit val Channels(
      _,
      compressedChannel,
      uncompressedBytes
    ) =
      Channels(path)

    val header = Header(path)
    val endPos = Pos(path.size, 0)

    implicit val contigLengths = header.contigLengths

    fileSplits
      .map {
        case split @ FileSplit(_, start, _, _) ⇒
          val bgzfBlockStart =
            FindBlockStart(
              path,
              start,
              compressedChannel,
              bgzfBlocksToCheck
            )

          uncompressedBytes.stopAt(Pos(split.end, 0))

          FindRecordStart(
            path,
            bgzfBlockStart
          )
      }
      .sliding2(endPos)
      .map(Split(_))
      .toVector
  }

  def getHadoopBamSplits(path: Path,
                         fileSplits: Seq[FileSplit])(
      implicit conf: Configuration
  ): Vector[Split] = {

    val ifmt = new BAMInputFormat
    val job = Job.getInstance(conf, s"$path:file-splits")
    setInputPaths(job, new fs.Path(path.uri))

    val javaSplits =
      fileSplits
        .map(x => (x: input.FileSplit): InputSplit)
        .toBuffer
        .asJava

    ifmt
      .getSplits(
        javaSplits,
        conf
      )
      .asScala
      .toVector
      .map(_.asInstanceOf[FileVirtualSplit]: Split)
  }
}
