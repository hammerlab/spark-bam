package org.hammerlab.bam.check

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.resources.tcgaBamExcerpt
import org.hammerlab.spark.test.suite.MainSuite
import org.hammerlab.test.resources.File

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
      """2580596 uncompressed positions
        |941K compressed
        |Compression ratio: 2.68
        |7976 reads
        |9 seqdoop-only calls, 0 eager-only calls:
        |9 seqdoop-only calls:
        |	39374:30965:	1 before D0N7FACXX120305:6:2301:3845:171905 2/2 76b unmapped read. (placed at 1:24795617). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	366151:51533:	1 before C0FR5ACXX120302:4:1204:6790:58160 2/2 76b unmapped read. (placed at 1:24932215). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	391261:35390:	1 before C0FR5ACXX120302:4:1106:5132:48894 2/2 76b unmapped read. (placed at 1:24969786). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	463275:65228:	1 before D0N7FACXX120305:4:1102:8753:123279 2/2 76b unmapped read. (placed at 1:24973169). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	486847:6:	1 before D0N7FACXX120305:7:1206:9262:21623 2/2 76b unmapped read. (placed at 1:24973169). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	731617:46202:	1 before D0N7FACXX120305:7:2107:8337:34383 2/2 76b unmapped read. (placed at 1:24981330). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	755781:56269:	1 before C0FR5ACXX120302:4:2202:2280:16832 2/2 76b unmapped read. (placed at 1:24981398). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	780685:49167:	1 before D0N7FACXX120305:5:1204:3428:52534 2/2 76b unmapped read. (placed at 1:24981468). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	855668:64691:	1 before D0N7FACXX120305:5:1308:8464:128307 1/2 76b unmapped read. (placed at 1:24987247). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |"""
    )
  }

  test("tcga compare no .blocks file") {
    compare(
      File("1.2203053-2211029.noblocks.bam"),
      """2580596 uncompressed positions
        |941K compressed
        |Compression ratio: 2.68
        |7976 reads
        |9 seqdoop-only calls, 0 eager-only calls:
        |9 seqdoop-only calls:
        |	39374:30965:	1 before D0N7FACXX120305:6:2301:3845:171905 2/2 76b unmapped read. (placed at 1:24795617). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	366151:51533:	1 before C0FR5ACXX120302:4:1204:6790:58160 2/2 76b unmapped read. (placed at 1:24932215). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	391261:35390:	1 before C0FR5ACXX120302:4:1106:5132:48894 2/2 76b unmapped read. (placed at 1:24969786). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	463275:65228:	1 before D0N7FACXX120305:4:1102:8753:123279 2/2 76b unmapped read. (placed at 1:24973169). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	486847:6:	1 before D0N7FACXX120305:7:1206:9262:21623 2/2 76b unmapped read. (placed at 1:24973169). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	731617:46202:	1 before D0N7FACXX120305:7:2107:8337:34383 2/2 76b unmapped read. (placed at 1:24981330). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	755781:56269:	1 before C0FR5ACXX120302:4:2202:2280:16832 2/2 76b unmapped read. (placed at 1:24981398). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	780685:49167:	1 before D0N7FACXX120305:5:1204:3428:52534 2/2 76b unmapped read. (placed at 1:24981468). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
        |	855668:64691:	1 before D0N7FACXX120305:5:1308:8464:128307 1/2 76b unmapped read. (placed at 1:24987247). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
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
