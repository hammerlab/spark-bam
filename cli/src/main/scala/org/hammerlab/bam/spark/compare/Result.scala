package org.hammerlab.bam.spark.compare

import org.apache.hadoop.fs
import org.apache.hadoop.mapreduce.lib.input
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths
import org.apache.hadoop.mapreduce.{ InputSplit, Job }
import org.hammerlab.bam.check.{ MaxReadSize, ReadsToCheck }
import org.hammerlab.bam.header.Header
import org.hammerlab.bam.spark.load.Channels
import org.hammerlab.bam.spark.{ FindRecordStart, Split }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ BGZFBlocksToCheck, FindBlockStart }
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.{ FileSplit, FileSplits, MaxSplitSize }
import org.hammerlab.iterator.sliding.Sliding2Iterator._
import org.hammerlab.iterator.sorted.OrZipIterator._
import org.hammerlab.kryo._
import org.hammerlab.paths.Path
import org.hammerlab.timing.Timer
import org.hammerlab.types._
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit }
import shapeless.Generic

import scala.collection.JavaConverters._

case class Result(numSparkSplits: Int,
                  numHadoopSplits: Int,
                  diffs: Vector[Either[Split, Split]],
                  numSparkOnlySplits: Int,
                  numHadoopOnlySplits: Int,
                  hadoopBamMS: Int,
                  sparkBamMS: Int
                 )

object Result
  extends Timer {

  val gen = Generic[Result]

  def apply(path: Path)(
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

    val (hadoopBamMS, hadoopBamSplits) = time { getHadoopBamSplits(path, fileSplits) }

    val (sparkBamMS, sparkBamSplits) =  time { getSparkBamSplits(path, fileSplits) }

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
      numHadoopOnlySplits,
      hadoopBamMS.toInt,
      sparkBamMS.toInt
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
      compressedChannel,
      uncompressedBytes
    ) =
      Channels(path)

    /**
     * It's ok for different BAMs in one run to have contigs from different references / in different formats
     * ([[Header]] below instantiates [[org.hammerlab.genomics.reference.ContigName]]s).
     */
    import org.hammerlab.genomics.reference.ContigName.Normalization.Lenient

    val header = Header(path)
    val endPos = Pos(path.size, 0)

    implicit val contigLengths = header.contigLengths

    fileSplits
      .iterator
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

  implicit val alsoRegister: AlsoRegister[Result] =
    AlsoRegister(
      cls[Split]
    )
}
