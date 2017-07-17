package org.hammerlab.bam.check

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.resources.tcgaBamExcerpt
import org.hammerlab.spark.test.suite.MainSuite

class MainTest
  extends MainSuite(classOf[Registrar]) {

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

  test("tcga compare") {
    compare(
      tcgaBamExcerpt,
      """Seqdoop-only calls:
        |	39374:30965
        |	366151:51533
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

  test("tcga seqdoop") {
    val outputPath = tmpPath()

    Main.run(
      Args(
        blocksPerPartition = 5,
        seqdoop = true,
        out = Some(outputPath)
      ),
      Seq[String](tcgaBamExcerpt)
    )

    outputPath.read should be(
      """2580596 positions checked (7976 reads), 9 errors
        |False-call histogram:
        |	(9,FalsePositive)
        |
        |False calls:
        |	(39374:30965,FalsePositive)
        |	(366151:51533,FalsePositive)
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

  test("tcga eager") {
    val outputPath = tmpPath()

    Main.run(
      Args(
        blocksPerPartition = 5,
        eager = true,
        out = Some(outputPath)
      ),
      Seq[String](tcgaBamExcerpt)
    )

    outputPath.read should be(
      "2580596 positions checked (7976 reads), no errors!\n"
    )
  }

  test("tcga full") {
    val outputPath = tmpPath()

    Main.run(
      Args(
        blocksPerPartition = 5,
        full = true,
        out = Some(outputPath)
      ),
      Seq[String](tcgaBamExcerpt)
    )

    outputPath.read should be(
      "2580596 positions checked (7976 reads), no errors!\n"
    )
  }
}
