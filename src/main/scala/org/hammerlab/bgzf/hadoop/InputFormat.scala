package org.hammerlab.bgzf.hadoop

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.hammerlab.bgzf.block.Block.MAX_BLOCK_SIZE
import org.hammerlab.bgzf.block.{ HeaderParseException, MetadataStream }
import org.hammerlab.hadoop.{ FileSplits, Path }
import org.hammerlab.iterator.Sliding2Iterator._

case class InputFormat(path: Path,
                       conf: Configuration,
                       bgzfBlockHeadersToCheck: Int = 5) {

  def nextBlockAlignment(path: Path,
                         start: Long,
                         in: FSDataInputStream): Long = {
    in.seek(start)

    val headerStream = MetadataStream(in, closeStream = false)

    var pos = 0
    while (pos < MAX_BLOCK_SIZE) {
      try {
        in.seek(start + pos)
        headerStream.clear()
        headerStream
          .take(bgzfBlockHeadersToCheck)
          .size
        return start + pos
      } catch {
        case _: HeaderParseException ⇒
          pos += 1
      }
    }

    throw HeaderSearchFailedException(path, start, pos)
  }

  lazy val splits: Seq[Split] = {

    val fileSplits = FileSplits(path, conf)

    val fs = path.getFileSystem(conf)

    val len = fs.getFileStatus(path).getLen

    val is = fs.open(path)

    val blockStarts =
      fileSplits
        .map(
          fileSplit ⇒
            nextBlockAlignment(
              fileSplit.getPath,
              fileSplit.getStart,
              is
            )
        )

    (for {
      ((start, end), fileSplit) ←
        blockStarts
          .sliding2(len)
          .zip(fileSplits.iterator)
      if end > start
    } yield
      Split(
        path,
        start,
        end - start,
        fileSplit.getLocations
      )
    )
    .toList
  }
}

case class HeaderSearchFailedException(path: Path,
                                       start: Long,
                                       positionsAttempted: Int)
  extends IOException(
    s"$path: failed to find BGZF header in $positionsAttempted bytes from $start"
  )

