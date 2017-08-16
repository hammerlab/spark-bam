package org.hammerlab.bam.check

import caseapp._
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bytes._
import org.hammerlab.magic.rdd.collect.CollectPartitionsRDD._
import org.hammerlab.paths.Path
import org.hammerlab.resources.{ tcgaBamExcerpt, tcgaBamExcerptUnindexed }
import org.hammerlab.spark.test.suite.KryoSparkSuite
import org.hammerlab.test.resources.File

class BlocksTCGATest
  extends BlocksTest(tcgaBamExcerpt) {
  override def allBlocks: Array[Array[Int]] =
    Array(
      Array(     0,  14146,  39374,  65429,  89707, 113583, 138333, 163285),
      Array(188181, 213608, 239479, 264771, 289818, 315322, 340348, 366151),
      Array(391261, 416185, 440006, 463275, 486847, 510891, 534950, 559983, 584037),
      Array(608466, 633617, 658113, 682505, 707074, 731617, 755781, 780685),
      Array(805727, 830784, 855668, 879910, 904062, 929182, 953497)
    )

  override def boundariesCase: Array[Array[Int]] =
    Array(
      Array(),
      Array(),
      Array( 14146),
      Array(),
      Array(289818),
      Array(),
      Array(),
      Array(315322)
    )
}

class UnindexedBlocksTCGATest
  extends BlocksTest(tcgaBamExcerptUnindexed) {
  override def allBlocks: Array[Array[Int]] =
    Array(
      Array(     0,  14146,  39374,  65429,  89707, 113583, 138333, 163285, 188181),
      Array(213608, 239479, 264771, 289818, 315322, 340348, 366151, 391261),
      Array(416185, 440006, 463275, 486847, 510891, 534950, 559983, 584037, 608466),
      Array(633617, 658113, 682505, 707074, 731617, 755781, 780685, 805727),
      Array(830784, 855668, 879910, 904062, 929182, 953497)
    )

  override def boundariesCase: Array[Array[Int]] =
    Array(
      Array(14146),
      Array(),
      Array(),
      Array(289818),
      Array(),
      Array(315322)
    )
}

abstract class BlocksTest(file: File)
  extends KryoSparkSuite {

  import ParseRanges.parser

  implicit val path = file.path

  override def registrar: Class[_ <: KryoRegistrator] = classOf[Registrar]

  def check(args: String*)(expected: Array[Array[Int]])(implicit parser: Parser[Args]): Unit =
    check(
      parser(args)
        .right
        .get
        ._1,
      expected
    )

  def check(args: Args, expected: Array[Array[Int]]): Unit = {
    Blocks(args)
      .map(_.start)
      .collectParts should be(expected)
  }

  def allBlocks: Array[Array[Int]]
  def boundariesCase: Array[Array[Int]]

  test("all blocks") {
    check(
      Args(
        splitSize = Some(200.KB)
      ),
      allBlocks
    )
  }

  test("header block only") {
    check(
      "-i", "0"
    )(
      Array(Array(0))
    )
  }

  test("intra-header-block range") {
    check(
      "-i", "0+10k"
    )(
      Array(Array(0))
    )
  }

  test("block boundaries") {
    check(
      "-i", "10k-39374,289818-315323",
      "-m", "10k"
    )(
      boundariesCase
    )
  }
}
