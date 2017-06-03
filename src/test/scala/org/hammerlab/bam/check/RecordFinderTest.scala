package org.hammerlab.bam.check

import org.apache.hadoop.conf.Configuration
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.SeekableByteStream
import org.hammerlab.hadoop.Path
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class RecordFinderTest
  extends Suite {

  test("EoF") {
    val recordFinder = new RecordFinder
    val conf = new Configuration
    val path = Path(File("5k.bam").uri)
    val uncompressedBytes =
      SeekableByteStream(
        path
          .getFileSystem(conf)
          .open(path)
      )

    uncompressedBytes.seek(Pos(1006167, 15243))

    recordFinder(uncompressedBytes, Map()) should be(
      Some(
        Error(
          tooFewFixedBlockBytes = true,
          None, None, None, None, false
        )
      )
    )
  }
}
