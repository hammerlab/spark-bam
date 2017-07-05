package org.hammerlab.bgzf.index

import caseapp.{ ExtraName ⇒ O }
import org.hammerlab.app.{ IndexingApp, OutPathArgs }
import org.hammerlab.bgzf.block.{ Metadata, MetadataStream }
import org.hammerlab.io.Printer._
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
 * @param out path to write bgzf-block-positions to
 */
case class Args(@O("o") out: Option[Path] = None)
  extends OutPathArgs

object IndexBlocks
  extends IndexingApp[Args](".blocks") {

  override def run(args: Args): Unit = {

    val ch: ByteChannel = (path: SeekableByteChannel)

    val stream = MetadataStream(ch)

    var idx = 0

    heartbeat(
      () ⇒
        info(
          s"$idx blocks processed, ${ch.position()} bytes"
        ),
        for {
          Metadata(start, compressedSize, uncompressedSize) ← stream
        } {
          echo(s"$start,$compressedSize,$uncompressedSize")
          idx += 1
        }
    )

    info("Traversal done")
  }
}
