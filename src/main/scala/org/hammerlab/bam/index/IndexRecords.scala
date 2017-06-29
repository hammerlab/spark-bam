package org.hammerlab.bam.index

import java.io.{ IOException, PrintWriter }

import caseapp.{ ExtraName ⇒ O, _ }
import grizzled.slf4j.Logging
import htsjdk.samtools.util.{ RuntimeEOFException, RuntimeIOException }
import org.apache.hadoop.fs.{ Path ⇒ HPath }
import org.hammerlab.bam.iterator.{ PosStream, RecordStream, SeekablePosStream, SeekableRecordStream }
import org.hammerlab.bgzf.Pos
import org.hammerlab.hadoop.{ Configuration, Path }
import org.hammerlab.io.SeekableByteChannel
import org.hammerlab.timing.Interval.heartbeat

/**
 * Traverse a BAM file (the sole argument) and output the BGZP "virtual" positions ([[Pos]]) of all record-starts.
 *
 * @param outFile      File to write read-boundary [[Pos]]s to
 * @param parseRecords If true, parse [[htsjdk.samtools.SAMRecord]]s into memory while traversing, for minimal
 *                     sanity-checking
 * @param useChannel   If true, open/traverse the file using a [[java.nio.channels.FileChannel]] (default:
 *                     [[java.io.InputStream]]).
 * @param throwOnTruncation If true, throw an [[IOException]] in case of an unexpected EOF; default: stop traversing,
 *                          output only through end of last complete record, exit 0.
 */
case class Args(@O("o") outFile: Option[String] = None,
                @O("r") parseRecords: Boolean = false,
                @O("c") useChannel: Boolean = false,
                @O("t") throwOnTruncation: Boolean = false)

object IndexRecords
  extends CaseApp[Args]
    with Logging {

  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {
    implicit val conf = Configuration()

    if (remainingArgs.remainingArgs.size != 1) {
      throw new IllegalArgumentException(
        s"Exactly one argument (a BAM file path) is required"
      )
    }

    val path = Path(remainingArgs.remainingArgs.head)

    val stream =
      (args.parseRecords, args.useChannel) match {
        case (true, true) ⇒
          SeekableRecordStream(path).map(_._1)
        case (true, false) ⇒
          RecordStream(path.open).map(_._1)
        case (false, true) ⇒
          SeekablePosStream(path)
        case (false, false) ⇒
          PosStream(path.open)
      }

    var idx = 0
    var lastPos = Pos(0, 0)

    val outPath: Path =
      args
        .outFile
        .map(Path(_))
        .getOrElse(
          path.suffix(".records")
        )

    val fs = outPath.filesystem
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
