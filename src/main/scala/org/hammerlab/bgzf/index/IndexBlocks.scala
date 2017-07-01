package org.hammerlab.bgzf.index

import java.io.PrintWriter

import caseapp.{ CaseApp, RemainingArgs, ExtraName ⇒ O }
import grizzled.slf4j.Logging
import org.hammerlab.bgzf.block.{ Metadata, MetadataStream }
import org.hammerlab.io.{ ByteChannel, SeekableByteChannel }
import org.hammerlab.paths.Path
import org.hammerlab.timing.Interval.heartbeat

/**
 * CLI app for recording the offsets of all bgzf-block start-positions in a bgzf-compressed file.
 *
 * Format of output file is:
 *
 * <position>,<compressed block size>,<uncompressed block size>
 *
 * @param outFile path to write bgzf-block-positions to
 */
case class Args(@O("o") outFile: Option[Path] = None)

object IndexBlocks
  extends CaseApp[Args]
    with Logging {

  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {

    if (remainingArgs.remainingArgs.size != 1) {
      throw new IllegalArgumentException(
        s"Exactly one argument (a BAM file path) is required"
      )
    }

    val path = Path(remainingArgs.remainingArgs.head)

    val ch: ByteChannel = (path: SeekableByteChannel)

    val stream = MetadataStream(ch)

    val outPath: Path =
      args
        .outFile
        .getOrElse(
          path + ".blocks"
        )

    val out = new PrintWriter(outPath.outputStream)

    var idx = 0

    heartbeat(
      () ⇒
        info(
          s"$idx blocks processed, ${ch.position()} bytes"
        ),
        for {
          Metadata(start, compressedSize, uncompressedSize) ← stream
        } {
          out.println(s"$start,$compressedSize,$uncompressedSize")
          idx += 1
        }
    )

    info("Traversal done")
    out.flush()
    out.close()
  }
}
