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
        File("5k.bam").uri.toString,
        outFile = Some(outFile.uri.toString)
      ),
      RemainingArgs(Nil, Nil)
    )

    outFile should fileMatch("5k.bam.records")
  }
}
