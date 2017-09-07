package org.hammerlab.bam.check

import java.io.FileNotFoundException
import java.net.URI

import caseapp._
import org.apache.hadoop.fs
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{ FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path ⇒ HPath }
import org.apache.hadoop.util.Progressable
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.test.resources.{ tcgaBamExcerpt, tcgaBamExcerptUnindexed }
import org.hammerlab.magic.rdd.collect.CollectPartitionsRDD._
import org.hammerlab.paths.Path
import org.hammerlab.spark.test.suite.KryoSparkSuite

import scala.collection.mutable

class BlocksTCGATest
  extends BlocksTest()(tcgaBamExcerpt) {

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
  extends BlocksTest()(tcgaBamExcerptUnindexed) {

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

abstract class BlocksTest(implicit path: Path)
  extends KryoSparkSuite {

  sparkConf(
    "spark.hadoop.fs.jar.impl" → classOf[JarFileSystem].getCanonicalName
  )

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

/*
  test("hpath") {
    def exists(hpath: HPath, expected: Boolean = true): Unit = {
      hpath.getFileSystem(sc.hadoopConfiguration).exists(hpath) should be(expected)
    }

    exists(new HPath(tcgaBamExcerpt.uri))
    val blocksPath = tcgaBamExcerpt + ".blocks"
    val blocksURI = blocksPath.uri
    exists(new HPath(blocksURI))

    val s2 = "jar:file:///Users/ryan/.m2/repository/org/hammerlab/bam/test-bams_2.11/1.0.0-SNAPSHOT/test-bams_2.11-1.0.0-SNAPSHOT.jar!/1.2203053-2211029.bam.blocks"
    val u2 = new URI(s2)
    exists(new HPath(u2))
//    exists(new HPath(s2))

    val s3 = "jar:/Users/ryan/.m2/repository/org/hammerlab/bam/test-bams_2.11/1.0.0-SNAPSHOT/test-bams_2.11-1.0.0-SNAPSHOT.jar!/1.2203053-2211029.bam.blocks"
    val u3 = new URI(s3)
    exists(new HPath(u3))
    exists(new HPath(s3))

    val str = "jar:file://Users/ryan/.m2/repository/org/hammerlab/bam/test-bams_2.11/1.0.0-SNAPSHOT/test-bams_2.11-1.0.0-SNAPSHOT.jar!/1.2203053-2211029.bam.blocks"
    val uri = new URI(str)
    exists(new HPath(uri))
    exists(new HPath(str))
  }
*/

  test("all blocks") {
    check(
      "-m", "200k"
    )(
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

class JarFileSystem
  extends FileSystem {
  override def getFileStatus(f: fs.Path): FileStatus = {
    val uri = f.toUri
    val path = Path(uri)
    if (!path.exists)
      throw new FileNotFoundException(s"$uri")

    val ssp = uri.getRawSchemeSpecificPart
    val jarUri = new URI(ssp.substring(0, ssp.indexOf("!/")))
    val jarPath = new HPath(jarUri)
    val status =
      jarPath
        .getFileSystem(getConf)
        .getFileStatus(jarPath)

    status.setPath(f)

    status
  }

  override def open(f: fs.Path, bufferSize: Int): FSDataInputStream = {
    val path = Path(f.toUri)
    new FSDataInputStream(path.inputStream)
  }

  override def mkdirs(f: fs.Path, permission: FsPermission): Boolean = ???
  override def rename(src: fs.Path, dst: fs.Path): Boolean = ???
  override def listStatus(f: fs.Path): Array[FileStatus] = ???
  override def create(f: fs.Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = ???
  override def getWorkingDirectory: fs.Path = ???
  override def setWorkingDirectory(new_dir: fs.Path): Unit = ???
  override def getUri: URI = ???
  override def delete(f: fs.Path, recursive: Boolean): Boolean = ???
  override def append(f: fs.Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = ???
}
