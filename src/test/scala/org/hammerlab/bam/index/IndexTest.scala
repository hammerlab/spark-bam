package org.hammerlab.bam.index

import org.hammerlab.resources.bam5k
import org.hammerlab.test.Suite

class IndexTest
  extends Suite {
  test("5k.bam.bai") {
    val index = Index(bam5k)
    val references = index.references
    val chunks = index.chunks
    chunks.length should be(6)
  }
}
