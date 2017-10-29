package org.hammerlab.bam.rewrite

import caseapp.{ ExtraName ⇒ O, _ }
import grizzled.slf4j.Logging
import htsjdk.samtools.{ SAMFileWriterFactory, SamReaderFactory }
import org.hammerlab.args.IntRanges
import org.hammerlab.bam.index.IndexRecords
import org.hammerlab.bgzf.index.IndexBlocks
import org.hammerlab.cli.app.{ Cmd, HasPrinter, RequiredArgOutPathApp }
import org.hammerlab.cli.args.PrinterArgs

import scala.collection.JavaConverters._

/**
 * App that reads a BAM file and rewrites it after a trip through HTSJDK's BAM-reading/-writing machinery.
 *
 * Useful for creating BAM files with records that span BGZF block boundaries, which samtools won't do.
 *
 * This is necessary to test out [[org.hammerlab.bam.check.Checker]] machinery.
 */
object HTSJDKRewrite extends Cmd {
  /**
   * @param readRanges if set, only output ranges in these intervals / at these positions
   * @param indexBlocks if set, compute a ".blocks" file with positions and sizes of BGZF blocks in the output BAM; see
   *                    [[IndexBlocks]]
   * @param indexRecords if set, compute a ".records" file with positions of BAM records in the output BAM; see
   *                     [[IndexRecords]]
   */
  @AppName("Rewrite BAM file with HTSJDK; records not aligned to BGZF-block boundaries")
  @ProgName("… org.hammerlab.bam.rewrite.Main")
  case class Opts(@O("r") readRanges: Option[IntRanges] = None,
                  @Recurse printerArgs: PrinterArgs,
                  @O("b") indexBlocks: Boolean = false,
                  @O("i") indexRecords: Boolean = false)

  val main = Main(
    args ⇒ new RequiredArgOutPathApp(args)
      with HasPrinter
      with Logging {

      val reader = SamReaderFactory.make().open(path)

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
          .makeBAMWriter(header, true, printer)

      records.foreach(writer.addAlignment)

      reader.close()
      writer.close()

      if (args.indexBlocks)
        IndexBlocks.main(
          args.printerArgs,
          RemainingArgs(
            Seq(out.toString),
            Nil
          )
        )

      if (args.indexRecords)
        IndexRecords.main(
          IndexRecords.Opts(args.printerArgs),
          RemainingArgs(
            Seq(out.toString),
            Nil
          )
        )
    }
  )
//  object Main extends app.Main(App)
}
