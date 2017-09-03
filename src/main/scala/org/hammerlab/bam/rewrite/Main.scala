package org.hammerlab.bam.rewrite

import caseapp.core.ArgParser
import caseapp.{ ExtraName ⇒ O, _ }
import grizzled.slf4j.Logging
import htsjdk.samtools.{ SAMFileWriterFactory, SamReaderFactory }
import org.hammerlab.args.IntRanges
import org.hammerlab.bam.index.IndexRecords
import org.hammerlab.bgzf.index.IndexBlocks
import org.hammerlab.paths.Path
import org.hammerlab.{ bam, bgzf }

import scala.collection.JavaConverters._

/**
 * @param readRanges if set, only output ranges in these intervals / at these positions
 * @param overwrite if set, overwrite an existing file at the specified output path
 * @param indexBlocks if set, compute a ".blocks" file with positions and sizes of BGZF blocks in the output BAM; see
 *                    [[IndexBlocks]]
 * @param indexRecords if set, compute a ".records" file with positions of BAM records in the output BAM; see
 *                     [[IndexRecords]]
 */
@AppName("Rewrite BAM file with HTSJDK; records not aligned to BGZF-block boundaries")
@ProgName("… org.hammerlab.bam.rewrite.Main")
case class Args(@O("r") readRanges: Option[IntRanges] = None,
                @O("f") overwrite: Boolean = false,
                @O("b") indexBlocks: Boolean = false,
                @O("i") indexRecords: Boolean = false)

/**
 * App that reads a BAM file and rewrites it after a trip through HTSJDK's BAM-reading/-writing machinery.
 *
 * Useful for creating BAM files with records that span BGZF block boundaries, which samtools won't do.
 *
 * This is necessary to test out [[org.hammerlab.bam.check.Checker]] machinery.
 */
object Main
  extends CaseApp[Args]
    with Logging {

  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {
    remainingArgs.remainingArgs match {
      case Seq(in, out) ⇒
        val inPath = Path(in)
        val outPath = Path(out)
        if (outPath.exists && !args.overwrite)
          throw new IllegalArgumentException(
            s"Output path $outPath already exists"
          )

        val readerFactory = SamReaderFactory.make()

        val reader = readerFactory.open(inPath)

        val header = reader.getFileHeader

        val records = {
          val records =
            reader
              .iterator()
              .asScala

          args.readRanges match {
            case Some(ranges) ⇒
              records
                .zipWithIndex
                .collect {
                  case (record, idx) if ranges.contains(idx) ⇒
                    record
                }
            case _ ⇒
              records
          }
        }

        val writer =
          new SAMFileWriterFactory()
            .makeBAMWriter(header, true, outPath.outputStream)

        records.foreach(writer.addAlignment)

        reader.close()
        writer.close()

        if (args.indexBlocks)
          IndexBlocks.run(
            bgzf.index.Args(),
            RemainingArgs(
              Seq(outPath.toString()),
              Nil
            )
          )

        if (args.indexRecords)
          IndexRecords.run(
            bam.index.Args(),
            RemainingArgs(
              Seq(outPath.toString()),
              Nil
            )
          )
      case _ ⇒
        error("Usage: <in.bam> <out.bam>")
        exit(1)
    }
  }
}
