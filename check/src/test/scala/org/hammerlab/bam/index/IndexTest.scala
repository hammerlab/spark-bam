package org.hammerlab.bam.index

import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.bgzf.Pos
import org.hammerlab.test.Suite

class IndexTest
  extends Suite {
  test("2.bam.bai") {
    val index = Index(bam2)
    val references = index.references
    index.chunks should be(
      Vector(
        Chunk(Pos(     0,  5650), Pos(287788, 31300)),
        Chunk(Pos(287788, 30115), Pos(314028, 45444)),
        Chunk(Pos(439897, 20150), Pos(439897, 39777)),
        Chunk(Pos(314028, 45444), Pos(439897, 20150)),
        Chunk(Pos(439897, 39777), Pos(531725, 0))
      )
    )
  }
}
