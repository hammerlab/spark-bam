package org.hammerlab.bam.check

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.resources.{ tcgaBamExcerpt, tcgaBamExcerptUnindexed }
import org.hammerlab.spark.test.suite.MainSuite

class MainTest
  extends MainSuite(classOf[Registrar]) {

  implicit def wrapOpt[T](t: T): Option[T] = Some(t)

  def compare(path: String, expected: String): Unit = {
    val outputPath = tmpPath()

    Main.main(
      Array(
        "-m", "200k",
        "-o", outputPath.toString,
        path
      )
    )

    outputPath.read should be(expected.stripMargin)
  }

  val seqdoopTCGAExpectedOutput =
    """2580596 uncompressed positions
      |941K compressed
      |Compression ratio: 2.68
      |7976 reads
      |9 false positives, 0 false negatives
      |
      |False-positive-site flags histogram:
      |	9:	tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
      |
      |False positives with succeeding read info:
      |	39374:30965:	1 before D0N7FACXX120305:6:2301:3845:171905 2/2 76b unmapped read (placed at 1:24795617). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
      |	366151:51533:	1 before C0FR5ACXX120302:4:1204:6790:58160 2/2 76b unmapped read (placed at 1:24932215). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
      |	391261:35390:	1 before C0FR5ACXX120302:4:1106:5132:48894 2/2 76b unmapped read (placed at 1:24969786). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
      |	463275:65228:	1 before D0N7FACXX120305:4:1102:8753:123279 2/2 76b unmapped read (placed at 1:24973169). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
      |	486847:6:	1 before D0N7FACXX120305:7:1206:9262:21623 2/2 76b unmapped read (placed at 1:24973169). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
      |	731617:46202:	1 before D0N7FACXX120305:7:2107:8337:34383 2/2 76b unmapped read (placed at 1:24981330). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
      |	755781:56269:	1 before C0FR5ACXX120302:4:2202:2280:16832 2/2 76b unmapped read (placed at 1:24981398). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
      |	780685:49167:	1 before D0N7FACXX120305:5:1204:3428:52534 2/2 76b unmapped read (placed at 1:24981468). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
      |	855668:64691:	1 before D0N7FACXX120305:5:1308:8464:128307 1/2 76b unmapped read (placed at 1:24987247). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
      |"""
    .stripMargin

  test("tcga compare") {
    compare(
      tcgaBamExcerpt,
      seqdoopTCGAExpectedOutput
    )
  }

  test("tcga compare no .blocks file") {
    compare(
      tcgaBamExcerptUnindexed,
      seqdoopTCGAExpectedOutput
    )
  }

  test("tcga seqdoop") {
    val outputPath = tmpPath()

    Main.main(
      Array(
        "-s",
        "-m", "200k",
        "-o", outputPath.toString,
        tcgaBamExcerpt
      )
    )

    outputPath.read should be(seqdoopTCGAExpectedOutput)
  }

  test("tcga eager") {
    val outputPath = tmpPath()

    Main.main(
      Array(
        "-e",
        "-m", "200k",
        "-o", outputPath.toString,
        tcgaBamExcerpt
      )
    )

    outputPath.read should be(
      """2580596 uncompressed positions
        |941K compressed
        |Compression ratio: 2.68
        |7976 reads
        |All calls matched!
        |"""
      .stripMargin
    )
  }
}
