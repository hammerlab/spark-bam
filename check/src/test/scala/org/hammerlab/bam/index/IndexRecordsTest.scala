package org.hammerlab.bam.index

import org.hammerlab.bam.index.IndexRecords.Main
import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.cli.app.MainSuite

class IndexRecordsTest
  extends MainSuite(Main) {
  test("2.bam") {
    checkFile(
      bam2
    )(
      "2.bam.records"
    )
  }
}
