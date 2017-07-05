package org.hammerlab.bam.check

import java.lang.System.setProperty

import org.hammerlab.spark.test.suite.SparkConfBase
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class MainTest
  extends Suite
    with SparkConfBase {

  setProperty("spark.driver.allowMultipleContexts", "true")

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

  test("1.2203053-2211029.bam") {
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
}
