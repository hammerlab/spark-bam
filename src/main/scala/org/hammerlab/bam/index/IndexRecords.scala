package org.hammerlab.bam.index

import java.io.{ IOException, PrintWriter }

import caseapp.{ ExtraName ⇒ O, _ }
import grizzled.slf4j.Logging
import htsjdk.samtools.util.{ RuntimeEOFException, RuntimeIOException }
import org.hammerlab.HadoopApp
import org.hammerlab.bam.iterator.{ PosStream, RecordStream, SeekablePosStream, SeekableRecordStream }
import org.hammerlab.bgzf.Pos
import org.hammerlab.hadoop.Path
import org.hammerlab.timing.Interval.heartbeat

/**
 * Traverse a BAM file (the sole argument) and output the BGZP "virtual" positions ([[Pos]]) of all record-starts.
 *
 * @param outPath      File to write read-boundary [[Pos]]s to
 * @param parseRecords If true, parse [[htsjdk.samtools.SAMRecord]]s into memory while traversing, for minimal
 *                     sanity-checking
 * @param useChannel   If true, open/traverse the file using a [[java.nio.channels.FileChannel]] (default:
 *                     [[java.io.InputStream]]).
 * @param throwOnTruncation If true, throw an [[IOException]] in case of an unexpected EOF; default: stop traversing,
 *                          output only through end of last complete record, exit 0.
 */
case class Args(@O("o") outPath: Option[Path] = None,
                @O("r") parseRecords: Boolean = false,
                @O("c") useChannel: Boolean = false,
                @O("t") throwOnTruncation: Boolean = false)

object IndexRecords
  extends HadoopApp[Args]
    with Logging {

  override def run(args: Args, remainingArgs: Seq[String]): Unit = {

    if (remainingArgs.size != 1) {
      throw new IllegalArgumentException(
        s"Exactly one argument (a BAM file path) is required"
      )
    }

    val path = Path(remainingArgs.head)

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

    val outPath: Path =
      args
      .outPath
        .getOrElse(
          path.suffix(".records")
        )

    val out = new PrintWriter(outPath.outputStream)

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
        info(
          s"$idx records processed, pos: $lastPos"
        ),
      if (args.throwOnTruncation) {
        traverse()
      } else {
        try {
          traverse()
        } catch {
          case e: IOException ⇒
            error(e)

          // Non-record-parsing mode hits this in the case of a truncated file
          case e: RuntimeEOFException ⇒
            error(e)

          // Record-parsing can hit this in the presence of incomplete records in a truncated file
          case e: RuntimeIOException ⇒
            error(e)
        }
      }
    )

    info("Traversal done")
    out.flush()
    out.close()
  }
}
