package org.hammerlab.bam.check.seqdoop

import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.hadoop.Configuration
import org.hammerlab.test.Suite
import org.hammerlab.resources.tcgaBamExcerpt

class CheckerTest
  extends Suite {

  val path = tcgaBamExcerpt
  implicit val conf = Configuration()
  val contigLengths = ContigLengths(path)
  val checker = Checker(path, contigLengths)

  test("486847:6") {
    checker(Pos(486847, 6)) should be(true)  // false positive
  }
}
