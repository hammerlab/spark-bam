package org.hammerlab.bgzf.index

import org.hammerlab.hadoop.{ Configuration, Path }
import org.hammerlab.test.Suite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.File

class IndexBlocksTest
  extends Suite {

  implicit val conf = Configuration()

  test("5k.bam") {
    val outPath = tmpPath()
    IndexBlocks.run(
      Args(
        outFile = Some(Path(outPath.uri))
      ),
      Seq(File("5k.bam"))
    )

    outPath should fileMatch("5k.bam.blocks")
  }
}
