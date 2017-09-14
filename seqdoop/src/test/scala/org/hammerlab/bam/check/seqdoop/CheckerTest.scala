package org.hammerlab.bam.check.seqdoop

import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bam.test.resources.bam1
import org.hammerlab.bgzf.Pos
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.hadoop.Configuration
import org.hammerlab.test.Suite

class CheckerTest
  extends Suite {

  val path = bam1
  val ch = SeekableByteChannel(path).cache
  implicit val conf = Configuration()
  val contigLengths = ContigLengths(path)
  val checker = Checker(path, ch, contigLengths)

  test("486847:6") {
    checker(Pos(486847, 6)) should be(true)  // false positive
  }

  override def afterAll(): Unit = {
    super.afterAll()
    ch.close()
  }
}
