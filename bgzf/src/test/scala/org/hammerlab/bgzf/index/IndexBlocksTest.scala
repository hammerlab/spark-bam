package org.hammerlab.bgzf.index

import caseapp.RemainingArgs
import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.test.Suite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch

class IndexBlocksTest
  extends Suite {

  test("2.bam") {
    val outPath = tmpPath()
    IndexBlocks.run(
      Args(
        out = Some(outPath)
      ),
      RemainingArgs(
        Seq(bam2.toString),
        Nil
      )
    )

    outPath should(fileMatch("2.bam.blocks"))
  }
}
