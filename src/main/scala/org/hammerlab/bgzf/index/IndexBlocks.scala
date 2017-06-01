package org.hammerlab.bgzf.index

import java.io.{ IOException, PrintWriter }
import java.net.URI
import java.nio.channels.FileChannel

import caseapp._
import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.hammerlab.bgzf.block.{ Metadata, MetadataStream }
import org.hammerlab.io.ByteChannel
import org.hammerlab.paths.Path
import org.hammerlab.timing.Interval.heartbeat

case class Args(@ExtraName("b") bamFile: String,
                @ExtraName("o") outFile: Option[String] = None,
                @ExtraName("c") useChannel: Boolean = false,
                @ExtraName("i") omitEmptyFinalBlock: Boolean = false)

object IndexBlocks
  extends CaseApp[Args]
    with Logging {

  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {
    val conf = new Configuration
    val path = Path(new URI(args.bamFile))
    val ch: ByteChannel =
      if (args.useChannel)
        FileChannel.open(path)
      else
        path.inputStream

    val stream =
      MetadataStream(
        ch,
        includeEmptyFinalBlock = !args.omitEmptyFinalBlock
      )

    val outPath =
      Path(
        new URI(
          args
            .outFile
            .getOrElse(
              args.bamFile + ".blocks"
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
          Metadata(start, uncompressedSize, _) ← stream
        } {
          out.println(s"$start,$uncompressedSize")
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
