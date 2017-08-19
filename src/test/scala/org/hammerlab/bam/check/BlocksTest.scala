package org.hammerlab.bam.check

import caseapp._
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.args.SplitSize
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bytes._
import org.hammerlab.magic.rdd.collect.CollectPartitionsRDD._
import org.hammerlab.resources.{ tcgaBamExcerpt, tcgaBamExcerptUnindexed }
import org.hammerlab.spark.test.suite.KryoSparkSuite
import org.hammerlab.test.resources.File

class BlocksTCGATest
  extends BlocksTest(tcgaBamExcerpt) {

  override def boundariesCase: Array[Array[Int]] =
    Array(
      Array( 14146),
      Array(),
      Array(289818),
      Array(),
      Array(315322),
      Array(),
      Array(),
      Array()
    )

  override def boundariesCaseBounds: Seq[(Int, Option[Int])] =
    Seq(
          0 → Some(10240),
      10240 → Some(20480),
      20480 → Some(30720),
      30720 → Some(40960),
      40960 → Some(51200),
      51200 → Some(61440),
      61440 → Some(71680),
      71680 → Some(81920)
    )

}

class UnindexedBlocksTCGATest
  extends BlocksTest(tcgaBamExcerptUnindexed) {

  override def boundariesCase: Array[Array[Int]] =
    Array(
      Array(14146),
      Array(),
      Array(),
      Array(289818),
      Array(),
      Array(315322)
    )

  override def boundariesCaseBounds: Seq[(Int, Option[Int])] =
    Seq(
       10240 → Some( 20480),
       20480 → Some( 30720),
       30720 → Some( 40960),
      286720 → Some(296960),
      296960 → Some(307200),
      307200 → Some(317440)
    )
}

abstract class BlocksTest(file: File)
  extends KryoSparkSuite {

  implicit val path = file.path

  override def registrar: Class[_ <: KryoRegistrator] = classOf[Registrar]

  def check(
      args: String*
  )(
      expected: Array[Array[Int]],
      expectedBounds: (Int, Option[Int])*
  )(
      implicit
      parser: Parser[Blocks.Args]
  ): Unit =
    check(
      parser(args)
        .right
        .get
        ._1,
      expected,
      expectedBounds: _*
    )

  def check(implicit
            args: Blocks.Args,
            expectedBlocks: Array[Array[Int]],
            expectedBounds: (Int, Option[Int])*): Unit = {
    val (blocks, bounds) = Blocks()

    blocks
      .map(_.start)
      .collectParts should be(expectedBlocks)

    bounds.partitions should be(
      expectedBounds
        .map(Some(_))
    )
  }

  def boundariesCase: Array[Array[Int]]
  def boundariesCaseBounds: Seq[(Int, Option[Int])]

  test("all blocks") {
    check(
      Blocks.Args(
        splits = SplitSize.Args(
          splitSize = Some(200 KB)
        )
      ),
      Array(
        Array(     0,  14146,  39374,  65429,  89707, 113583, 138333, 163285, 188181),
        Array(213608, 239479, 264771, 289818, 315322, 340348, 366151, 391261),
        Array(416185, 440006, 463275, 486847, 510891, 534950, 559983, 584037, 608466),
        Array(633617, 658113, 682505, 707074, 731617, 755781, 780685, 805727),
        Array(830784, 855668, 879910, 904062, 929182, 953497)
      ),
           0 → Some( 204800),
      204800 → Some( 409600),
      409600 → Some( 614400),
      614400 → Some( 819200),
      819200 → Some(1024000)
    )
  }

  test("header block only") {
    check(
      "-i", "0"
    )(
      Array(Array(0)),
      0 → Some(2097152)
    )
  }

  test("intra-header-block range") {
    check(
      "-i", "0+10k"
    )(
      Array(Array(0)),
      0 → Some(2097152)
    )
  }

  test("block boundaries") {
    check(
      "-i", "10k-39374,289818-315323",
      "-m", "10k"
    )(
      boundariesCase,
      boundariesCaseBounds: _*
    )
  }
}
