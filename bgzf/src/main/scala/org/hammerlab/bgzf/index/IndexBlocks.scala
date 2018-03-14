package org.hammerlab.bgzf.index

import caseapp.{ AppName, ProgName }
import grizzled.slf4j.Logging
import org.hammerlab.bgzf.block.{ Metadata, MetadataStream }
import org.hammerlab.channel.ByteChannel
import org.hammerlab.cli.app.{ Cmd, IndexingApp }
import org.hammerlab.cli.args.PrinterArgs
import org.hammerlab.timing.Interval.heartbeat

object IndexBlocks extends Cmd {

  /**
   * CLI app for recording the offsets of all bgzf-block start-positions in a bgzf-compressed file.
   *
   * Format of output file is:
   *
   * <position>,<compressed block size>,<uncompressed block size>
   */
  @AppName("Iterate through and index the BGZF blocks in a BAM file")
  @ProgName("… org.hammerlab.bgzf.index.IndexBlocks")
  type Opts = PrinterArgs

  val main = Main(
    new IndexingApp[Opts]("blocks", _)
      with Logging {

      val ch: ByteChannel = path.inputStream

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

      ch.close()
      info("Traversal done")
    }
  )
}
