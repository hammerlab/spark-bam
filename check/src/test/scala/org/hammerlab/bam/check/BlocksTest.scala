package org.hammerlab.bam.check

import caseapp._
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.test.resources.{ TestBams, bam1Unindexed }
import org.hammerlab.magic.rdd.collect.CollectPartitionsRDD._
import org.hammerlab.paths.Path
import org.hammerlab.spark.test.suite.KryoSparkSuite

class IndexedBlocksTest
  extends BlocksTest
    with TestBams {
  
  override implicit def path: Path = bam1

  override def boundariesCaseBlocks: Array[Array[Int]] =
    Array(
      Array( 14146),
      Array(),
      Array(287709),
      Array(),
      Array(312794)
    )

  override def boundariesCaseBounds: Seq[(Int, Option[Int])] =
    Seq(
          0 → Some(10240),
      10240 → Some(20480),
      20480 → Some(30720),
      30720 → Some(40960),
      40960 → Some(51200)
    )
}

class UnindexedBlocksTest
  extends BlocksTest {

  override implicit def path: Path = bam1Unindexed

  override def boundariesCaseBlocks: Array[Array[Int]] =
    Array(
      Array( 14146),
      Array(),
      Array(),
      Array(287709),
      Array(),
      Array(312794)
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

abstract class BlocksTest
  extends KryoSparkSuite {

  implicit def path: Path

  register(Registrar)

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
    val Blocks(blocks, bounds) = Blocks()

    blocks
      .map(_.start)
      .collectParts should be(expectedBlocks)

    bounds.partitions should be(
      expectedBounds
        .map(Some(_))
    )
  }

  def boundariesCaseBlocks: Array[Array[Int]]
  def boundariesCaseBounds: Seq[(Int, Option[Int])]

  test("all blocks") {
    check(
      "-m", "100k"
    )(
      Array(
        Array(0, 14146, 39374, 65429, 89707),
        Array(113583, 138333, 163285, 188181),
        Array(213608, 239479, 263656, 287709),
        Array(312794, 336825, 361204, 386382),
        Array(410905, 435247, 459832, 484396, 508565),
        Array(533464, 558458, 583574)
      ),
           0 → Some( 102400),
      102400 → Some( 204800),
      204800 → Some( 307200),
      307200 → Some( 409600),
      409600 → Some( 512000),
      512000 → Some( 614400)
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
      "-i", "10k-39374,287709-312795",
      "-m", "10k"
    )(
      boundariesCaseBlocks,
      boundariesCaseBounds: _*
    )
  }
}
