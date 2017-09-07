package org.hammerlab.bgzf.index

import caseapp.RemainingArgs
import org.hammerlab.bam.test.resources.bam5k
import org.hammerlab.test.Suite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch

class IndexBlocksTest
  extends Suite {

  test("5k.bam") {
    val outPath = tmpPath()
    IndexBlocks.run(
      Args(
        out = Some(outPath)
      ),
      RemainingArgs(
        Seq(bam5k.toString),
        Nil
      )
    )

    outPath should(fileMatch("5k.bam.blocks"))
  }
}
