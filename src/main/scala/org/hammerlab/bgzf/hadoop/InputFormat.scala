package org.hammerlab.bgzf.hadoop

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.hammerlab.bgzf.block.FindBlockStart
import org.hammerlab.hadoop.{ FileSplits, Path }
import org.hammerlab.io.ByteChannel.SeekableHadoopByteChannel
import org.hammerlab.iterator.Sliding2Iterator._

object Splits {
  def apply(path: Path,
            conf: Configuration,
            bgzfBlockHeadersToCheck: Int = 5): Seq[Split] = {

    val fileSplits = FileSplits(path, conf)

    val fs = path.getFileSystem(conf)

    val len = fs.getFileStatus(path).getLen

    val is = SeekableHadoopByteChannel(path, conf)

    val blockStarts =
      fileSplits
        .map(
          fileSplit ⇒
            FindBlockStart(
              fileSplit.getPath,
              fileSplit.getStart,
              is,
              bgzfBlockHeadersToCheck
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

