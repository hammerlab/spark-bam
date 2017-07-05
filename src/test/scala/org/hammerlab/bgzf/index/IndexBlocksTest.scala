package org.hammerlab.bgzf.index

import caseapp.RemainingArgs
import org.hammerlab.test.Suite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.File

class IndexBlocksTest
  extends Suite {

  test("5k.bam") {
    val outPath = tmpPath()
    IndexBlocks.run(
      Args(
        out = Some(outPath)
      ),
      RemainingArgs(
        Seq(File("5k.bam")),
        Nil
      )
    )

    outPath should fileMatch("5k.bam.blocks")
  }
}
