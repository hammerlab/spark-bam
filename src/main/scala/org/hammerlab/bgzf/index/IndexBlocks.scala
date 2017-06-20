package org.hammerlab.bgzf.index

import java.io.{ IOException, PrintWriter }
import java.net.URI
import java.nio.channels.FileChannel

import caseapp.{ ExtraName ⇒ O, _ }
import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
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
 * @param inFile input bgzf-compressed file
 * @param outFile path to write bgzf-block-positions to
 * @param useChannel open [[inFile]] using [[FileChannel]] interface, as opposed to the default
 *                   [[java.nio.file.Files.newInputStream]]
 * @param includeEmptyFinalBlock whether to emit a record for the final, empty bgzf-block
 */
case class Args(@O("b") inFile: String,
                @O("o") outFile: Option[String] = None,
                @O("c") useChannel: Boolean = false,
                @O("i") includeEmptyFinalBlock: Boolean = false)

object IndexBlocks
  extends CaseApp[Args]
    with Logging {

  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {
    val conf = new Configuration
    val path = Path(new URI(args.inFile))
    val ch: ByteChannel =
      if (args.useChannel)
        FileChannel.open(path): SeekableByteChannel
      else
        path.inputStream

    val stream =
      MetadataStream(
        ch,
        includeEmptyFinalBlock = args.includeEmptyFinalBlock
      )

    val outPath =
      Path(
        new URI(
          args
            .outFile
            .getOrElse(
              args.inFile + ".blocks"
            )
        )
      )

    val out = new PrintWriter(outPath.outputStream)

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
