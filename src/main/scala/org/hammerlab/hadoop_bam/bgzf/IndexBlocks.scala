package org.hammerlab.hadoop_bam.bgzf

import java.io.{ IOException, PrintWriter }
import java.net.URI
import java.nio.channels.FileChannel

import caseapp._
import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.hammerlab.hadoop_bam.Timer.time
import org.hammerlab.hadoop_bam.bam.ByteChannel
import org.hammerlab.hadoop_bam.bgzf.block.{ Metadata, MetadataStream }
import org.hammerlab.paths.Path

case class IndexBlocksArgs(@ExtraName("b") bamFile: String,
                           @ExtraName("o") outFile: Option[String],
                           @ExtraName("c") useChannel: Boolean = false)

object IndexBlocks
  extends CaseApp[IndexBlocksArgs]
    with Logging {

  override def run(args: IndexBlocksArgs, remainingArgs: RemainingArgs): Unit = {
    val conf = new Configuration
    val path = Path(new URI(args.bamFile))
    val ch: ByteChannel =
      if (args.useChannel)
        FileChannel.open(path)
      else
        path.inputStream

    val stream = MetadataStream(ch)
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

    time(
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
