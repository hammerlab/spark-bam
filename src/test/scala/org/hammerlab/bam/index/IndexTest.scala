package org.hammerlab.bam.index

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class IndexTest
  extends Suite {
  test("5k.bam.bai") {
    val path = new Path(File("5k.bam.bai").uri)
    val conf = new Configuration
    val index = Index(path, conf)
    val references = index.references
    val chunks = index.chunks
    chunks.length should be(6)
  }
}
