package org.hammerlab.bam.check

import java.lang.System.setProperty

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.spark.test.suite.SparkConfBase
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class MainTest
  extends Suite
    with SparkConfBase {

  setProperty("spark.driver.allowMultipleContexts", "true")

  // Register this class as its own KryoRegistrator
  setProperty("spark.kryo.registrator", classOf[Registrar].getCanonicalName)
  setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  setProperty("spark.kryo.referenceTracking", "false")
  setProperty("spark.kryo.registrationRequired", "true")

  setSparkProps()

  def compare(path: String, expected: String): Unit = {
    val outputPath = tmpPath()

    Main.run(
      Args(
        blocksPerPartition = 5,
        eager = true,
        seqdoop = true,
        out = Some(outputPath)
      ),
      Seq(path)
    )

    outputPath.read should be(expected.stripMargin)
  }

  test("1.2203053-2211029.bam compare") {
    compare(
      File("1.2203053-2211029.bam"),
      """Seqdoop-only calls:
        |	391261:35390
        |	463275:65228
        |	486847:6
        |	731617:46202
        |	755781:56269
        |	780685:49167
        |	855668:64691
        |"""
    )
  }

  test("1.2203053-2211029.bam seqdoop") {
    val outputPath = tmpPath()

    Main.run(
      Args(
        blocksPerPartition = 5,
        seqdoop = true,
        out = Some(outputPath)
      ),
      Seq[String](File("1.2203053-2211029.bam"))
    )

    outputPath.read should be(
      """2580596 positions checked (7976 reads), 7 errors
        |False-call histogram:
        |	(7,FalsePositive)
        |
        |False calls:
        |	(391261:35390,FalsePositive)
        |	(463275:65228,FalsePositive)
        |	(486847:6,FalsePositive)
        |	(731617:46202,FalsePositive)
        |	(755781:56269,FalsePositive)
        |	(780685:49167,FalsePositive)
        |	(855668:64691,FalsePositive)
        |
        |"""
        .stripMargin
    )
  }

  test("1.2203053-2211029.bam eager") {
    val outputPath = tmpPath()

    Main.run(
      Args(
        blocksPerPartition = 5,
        eager = true,
        out = Some(outputPath)
      ),
      Seq[String](File("1.2203053-2211029.bam"))
    )

    outputPath.read should be(
      "2580596 positions checked (7976 reads), no errors!\n"
    )
  }

  test("1.2203053-2211029.bam full") {
    val outputPath = tmpPath()

    Main.run(
      Args(
        blocksPerPartition = 5,
        full = true,
        out = Some(outputPath)
      ),
      Seq[String](File("1.2203053-2211029.bam"))
    )

    outputPath.read should be(
      "2580596 positions checked (7976 reads), no errors!\n"
    )
  }
}
