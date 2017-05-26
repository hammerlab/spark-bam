package org.hammerlab.hadoop_bam.bam

import caseapp.RemainingArgs
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class IndexRecordsTest
  extends Suite {
  test("simple") {
    IndexRecords.run(
      IndexRecordsArgs(
        File("2.bam").uri.toString,
        "hadoop-bam/src/test/resources/records.test",
        false,
        false
      ),
      RemainingArgs(Nil, Nil)
    )
  }
}
