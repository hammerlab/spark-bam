package org.hammerlab.hadoop_bam.bam

import java.io.{ IOException, PrintWriter }
import java.net.URI

import caseapp._
import grizzled.slf4j.Logging
import htsjdk.samtools.util.{ RuntimeEOFException, RuntimeIOException }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path ⇒ HPath }
import org.hammerlab.hadoop_bam.Timer.time
import org.hammerlab.hadoop_bam.bgzf.Pos
import org.hammerlab.paths.Path

case class IndexRecordsArgs(@ExtraName("b") bamFile: String,
                            @ExtraName("o") outFile: Option[String],
                            @ExtraName("r") parseRecords: Boolean = false,
                            @ExtraName("c") useChannel: Boolean = false,
                            @ExtraName("t") throwOnIOIssue: Boolean = false)

object IndexRecords
  extends CaseApp[IndexRecordsArgs]
    with Logging {
  override def run(args: IndexRecordsArgs, remainingArgs: RemainingArgs): Unit = {
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

    time(
      () ⇒
        logger.info(
          s"$idx records processed, pos: $lastPos"
        ),
      if (args.throwOnIOIssue) {
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
