package org.hammerlab.bgzf.index

import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.cli.base.app.MainSuite

class IndexBlocksTest
  extends MainSuite(IndexBlocks) {
  test("2.bam") {
    checkFile(
      bam2
    )(
      "2.bam.blocks"
    )
  }
}
