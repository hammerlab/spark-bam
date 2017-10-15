package org.hammerlab.bgzf.index

import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.bgzf.index.IndexBlocks.Main
import org.hammerlab.cli.app.MainSuite

class IndexBlocksTest
  extends MainSuite(Main) {
  test("2.bam") {
    checkFile(
      bam2
    )(
      "2.bam.blocks"
    )
  }
}
