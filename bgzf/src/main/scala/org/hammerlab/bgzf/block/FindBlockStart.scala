package org.hammerlab.bgzf.block

import hammerlab.path._
import org.hammerlab.bgzf.block.Block.MAX_BLOCK_SIZE
import org.hammerlab.channel.SeekableByteChannel

object FindBlockStart {
  def apply(path: Path,
            start: Long,
            in: SeekableByteChannel,
            bgzfBlocksToCheck: BGZFBlocksToCheck): Long = {

    val headerStream = MetadataStream(in)

    var pos = 0

    while (pos < MAX_BLOCK_SIZE) {
      try {
        in.seek(start + pos)
        headerStream.clear()
        headerStream
          .take(bgzfBlocksToCheck.n)
          .size
        return start + pos
      } catch {
        case _: HeaderParseException ⇒
          pos += 1
      }
    }

    throw HeaderSearchFailedException(
      path,
      start,
      pos
    )
  }
}
