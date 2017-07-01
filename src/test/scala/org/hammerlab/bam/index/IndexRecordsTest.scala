package org.hammerlab.bam.index

import caseapp.RemainingArgs
import org.hammerlab.hadoop.{ Configuration, Path }
import org.hammerlab.test.Suite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.File

class IndexRecordsTest
  extends Suite {

  implicit val conf = Configuration()

  test("5k.bam") {
    val outFile = tmpPath()
    IndexRecords.run(
      Args(
        outPath = Some(Path(outFile.uri))
      ),
      Seq(File("5k.bam"))
    )

    outFile should fileMatch("5k.bam.records")
  }
}
