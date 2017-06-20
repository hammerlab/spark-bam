package org.hammerlab.bam.index

import java.io.{ IOException, PrintWriter }
import java.net.URI

import caseapp.{ ExtraName ⇒ O, _ }
import grizzled.slf4j.Logging
import htsjdk.samtools.util.{ RuntimeEOFException, RuntimeIOException }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path ⇒ HPath }
import org.hammerlab.timing.Interval.heartbeat
import org.hammerlab.bam.iterator.{ PosStream, RecordStream, SeekablePosStream, SeekableRecordStream }
import org.hammerlab.bgzf.Pos
import org.hammerlab.paths.Path

/**
 * Traverse a BAM file and output the BGZP "virtual" positions ([[Pos]]) of all record-starts.
 *
 * @param bamFile      BAM file to "index"
 * @param outFile      File to write read-boundary [[Pos]]s to
 * @param parseRecords If true, parse [[htsjdk.samtools.SAMRecord]]s into memory while traversing, for minimal
 *                     sanity-checking
 * @param useChannel   If true, open/traverse the file using a [[java.nio.channels.FileChannel]] (default:
 *                     [[java.io.InputStream]]).
 * @param throwOnTruncation If true, throw an [[IOException]] in case of an unexpected EOF; default: stop traversing,
 *                          output only through end of last complete record, exit 0.
 */
case class Args(@O("b") bamFile: String,
                @O("o") outFile: Option[String] = None,
                @O("r") parseRecords: Boolean = false,
                @O("c") useChannel: Boolean = false,
                @O("t") throwOnTruncation: Boolean = false)

object IndexRecords
  extends CaseApp[Args]
    with Logging {

  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {
    val conf = new Configuration
    val inPath = Path(new URI(args.bamFile))

    val stream =
      (args.parseRecords, args.useChannel) match {
        case (true, true) ⇒
          SeekableRecordStream(inPath).map(_._1)
        case (true, false) ⇒
          RecordStream(inPath).map(_._1)
        case (false, true) ⇒
          SeekablePosStream(inPath)
        case (false, false) ⇒
          PosStream(inPath)
      }

    var idx = 0
    var lastPos = Pos(0, 0)

    val outPath =
      new HPath(
        args
          .outFile
          .getOrElse(
            args.bamFile + ".records"
          )
      )

    val fs = outPath.getFileSystem(conf)
    val out = new PrintWriter(fs.create(outPath))

    def traverse(): Unit = {
      for {
        pos ← stream
      } {
        out.println(s"${pos.blockPos},${pos.offset}")
        lastPos = pos
        idx += 1
      }
    }

    heartbeat(
      () ⇒
        logger.info(
          s"$idx records processed, pos: $lastPos"
        ),
      if (args.throwOnTruncation) {
        traverse()
      } else {
        try {
          traverse()
        } catch {
          case e: IOException ⇒
            logger.error(e)

          // Non-record-parsing mode hits this in the case of a truncated file
          case e: RuntimeEOFException ⇒
            logger.error(e)

          // Record-parsing can hit this in the presence of incomplete records in a truncated file
          case e: RuntimeIOException ⇒
            logger.error(e)
        }
      }
    )

    logger.info("Traversal done")
    out.flush()
    out.close()
  }
}
