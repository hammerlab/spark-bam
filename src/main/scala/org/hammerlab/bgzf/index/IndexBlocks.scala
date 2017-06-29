package org.hammerlab.bgzf.index

import java.io.{ IOException, PrintWriter }

import caseapp.{ ExtraName ⇒ O, _ }
import grizzled.slf4j.Logging
import org.hammerlab.bgzf.block.{ Metadata, MetadataStream }
import org.hammerlab.hadoop.{ Configuration, Path }
import org.hammerlab.io.{ ByteChannel, SeekableByteChannel }
import org.hammerlab.timing.Interval.heartbeat

/**
 * CLI app for recording the offsets of all bgzf-block start-positions in a bgzf-compressed file.
 *
 * Format of output file is:
 *
 * <position>,<compressed block size>,<uncompressed block size>
 *
 * @param outFile path to write bgzf-block-positions to
 * @param includeEmptyFinalBlock whether to emit a record for the final, empty bgzf-block
 */
case class Args(@O("o") outFile: Option[String] = None,
                @O("i") includeEmptyFinalBlock: Boolean = false)

object IndexBlocks
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

    val ch: ByteChannel = SeekableByteChannel(path)

    val stream =
      MetadataStream(
        ch,
        includeEmptyFinalBlock = args.includeEmptyFinalBlock
      )

    val outPath =
      args
        .outFile
        .map(Path(_))
        .getOrElse(
          path.suffix(".blocks")
        )

    val out = new PrintWriter(path.filesystem.create(path))

    var idx = 0

    heartbeat(
      () ⇒
        logger.info(
          s"$idx blocks processed, ${ch.position()} bytes"
        ),
      try {
        for {
          Metadata(start, compressedSize, uncompressedSize) ← stream
        } {
          out.println(s"$start,$compressedSize,$uncompressedSize")
          idx += 1
        }
      } catch {
        case e: IOException ⇒
          logger.error(e)
      }
    )

    logger.info("Traversal done")
    out.flush()
    out.close()
  }
}
