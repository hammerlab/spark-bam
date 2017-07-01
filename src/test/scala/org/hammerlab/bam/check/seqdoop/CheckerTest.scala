package org.hammerlab.bam.check.seqdoop

import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bgzf.Pos
import org.hammerlab.hadoop.Configuration
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class CheckerTest
  extends Suite {

  implicit val conf = Configuration()
  val path = File("1.2205029-2209029.bam")
  val contigLengths = ContigLengths(path)
  val checker = Checker(path, contigLengths)

  test("0:0") {
    checker(Pos(0, 0)) should be(false)
  }

  test("441192:37166") {
    checker(Pos(441192, 37166)) should be(false)
  }

  test("225622:49212") {
    checker(Pos(225622, 49212)) should be(true)
  }
}
