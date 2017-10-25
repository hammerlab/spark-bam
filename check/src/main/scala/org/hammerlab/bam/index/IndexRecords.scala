package org.hammerlab.bam.index

import java.io.IOException

import caseapp.{ AppName, ProgName, Recurse, ExtraName ⇒ O }
import grizzled.slf4j.Logging
import htsjdk.samtools.util.{ RuntimeEOFException, RuntimeIOException }
import org.hammerlab.bam.iterator.{ PosStream, RecordStream, SeekablePosStream, SeekableRecordStream }
import org.hammerlab.bgzf.Pos
import org.hammerlab.cli.app
import org.hammerlab.cli.app.{ Args, Cmd, IndexingApp }
import org.hammerlab.cli.args.PrinterArgs
import org.hammerlab.io.Printer._
import org.hammerlab.timing.Interval.heartbeat

object IndexRecords extends Cmd {

  /**
   * Traverse a BAM file (the sole argument) and output the BGZF "virtual" positions ([[Pos]]) of all record-starts.
   *
   * @param parseRecords If true, parse [[htsjdk.samtools.SAMRecord]]s into memory while traversing, for minimal
   *                     sanity-checking
   * @param useChannel   If true, open/traverse the file using a [[java.nio.channels.FileChannel]] (default:
   *                     [[java.io.InputStream]]).
   * @param throwOnTruncation If true, throw an [[IOException]] in case of an unexpected EOF; default: stop traversing,
   *                          output only through end of last complete record, exit 0.
   */
  @AppName("Iterate through and index the records in a BAM file")
  @ProgName("… org.hammerlab.bam.index.IndexRecords")
  case class Opts(@Recurse printArgs: PrinterArgs,
                  @O("r") parseRecords: Boolean = false,
                  @O("c") useChannel: Boolean = false,
                  @O("t") throwOnTruncation: Boolean = false)

  val main = Main(
    args ⇒ new IndexingApp[Opts]("records", args)
      with Logging {

      val stream =
        (args.parseRecords, args.useChannel) match {
          case (true, true) ⇒
            SeekableRecordStream(path).map(_._1)
          case (true, false) ⇒
            RecordStream(path.inputStream).map(_._1)
          case (false, true) ⇒
            SeekablePosStream(path)
          case (false, false) ⇒
            PosStream(path.inputStream)
        }

      var idx = 0
      var lastPos = Pos(0, 0)

      def traverse(): Unit = {
        for {
          pos ← stream
        } {
          echo(s"${pos.blockPos},${pos.offset}")
          lastPos = pos
          idx += 1
        }
      }

      heartbeat(
        () ⇒
          info(
            s"$idx records processed, pos: $lastPos"
          ),
        if (args.throwOnTruncation) {
          traverse()
        } else {
          try {
            traverse()
          } catch {
            case e: IOException ⇒ error(e)

            // Non-record-parsing mode hits this in the case of a truncated file
            case e: RuntimeEOFException ⇒ error(e)

            // Record-parsing can hit this in the presence of incomplete records in a truncated file
            case e: RuntimeIOException ⇒ error(e)
          }
        }
      )

      info("Traversal done")
    }
  )
//  object Main extends app.Main(App)
}
