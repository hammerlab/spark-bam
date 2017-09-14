package org.hammerlab.bam.index

import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.test.Suite

class IndexTest
  extends Suite {
  test("5k.bam.bai") {
    val index = Index(bam2)
    val references = index.references
    val chunks = index.chunks
    chunks.length should be(6)
  }
}
