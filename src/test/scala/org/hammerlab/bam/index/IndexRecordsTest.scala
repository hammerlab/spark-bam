package org.hammerlab.bam.index

import caseapp.RemainingArgs
import org.hammerlab.test.Suite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.File

class IndexRecordsTest
  extends Suite {
  test("5k.bam") {
    val outFile = tmpPath()
    IndexRecords.run(
      Args(
        outFile = Some(outFile.uri.toString)
      ),
      RemainingArgs(
        Seq(File("5k.bam")),
        Nil
      )
    )

    outFile should fileMatch("5k.bam.records")
  }
}
