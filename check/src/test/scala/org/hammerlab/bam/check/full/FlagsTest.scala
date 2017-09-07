package org.hammerlab.bam.check.full

import cats.syntax.all._
import org.hammerlab.bam.check.full.error.Flags
import org.hammerlab.test.Suite

import scala.collection.immutable.BitSet

class FlagsTest
  extends Suite {
  test("show") {
    Flags.fromBitSet(
      BitSet(
        1, 2, 3
      ) → 0
    )
    .show should be(
      "negativeReadIdx,tooLargeReadIdx,negativeReadPos"
    )
  }
}
