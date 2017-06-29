package org.hammerlab.bam.index

import org.hammerlab.hadoop.{ Configuration, Path }
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class IndexTest
  extends Suite {
  test("5k.bam.bai") {
    val path = Path(File("5k.bam.bai").uri)
    implicit val conf = Configuration()
    val index = Index(path)
    val references = index.references
    val chunks = index.chunks
    chunks.length should be(6)
  }
}
