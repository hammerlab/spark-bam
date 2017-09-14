package org.hammerlab.bam.check.full

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.bam.test.resources.TestBams
import org.hammerlab.paths.Path
import org.hammerlab.spark.test.suite.MainSuite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.File

class MainTest
  extends MainSuite(classOf[Registrar])
    with TestBams {

  def expected(basename: String) = File(s"output/full-check/$basename")

  def check(
      path: Path
  )(
      args: String*
  )(
      expected: File
  ): Unit = {
    val outputPath = tmpPath()

    Main.main(
      args.toArray ++
        Array(
          "-l", "10",
          "-o", outputPath.toString,
          path.toString
        )
    )

    outputPath should fileMatch(expected)
  }


/*
  test("tcga excerpt with indexed records") {
    check(
      tcgaBamExcerpt
    )(
      "-m", "200k"
    )(
      expected("tcga-indexed")
    )
  }

  test("tcga excerpt without indexed records") {
    check(
      tcgaBamExcerptUnindexed
    )(
      "-m", "200k"
    )(
      expected("tcga-unindexed")
    )
  }
*/

  test("5k.bam header block") {
    check(
      bam5k
    )(
      "-i", "0"
    )(
      expected("5k.bam.header")
    )
  }

  test("5k.bam second block, with reads") {
    check(
      bam5k
    )(
      "-i", "27784"
    )(
      expected("5k.bam.2nd-block")
    )
  }

  test("5k.bam 200k") {
    check(
      bam5k
    )(
      "-i", "0-200k",
      "-m", "100k"
    )(
      expected("5k.bam.200k")
    )
  }

/*
  test("5k.bam all") {
    check(
      bam5k
    )(
      // All default args
    )(
      expected("5k.bam")
    )
  }
*/
}
