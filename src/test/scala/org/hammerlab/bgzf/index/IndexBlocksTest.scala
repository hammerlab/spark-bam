package org.hammerlab.bgzf.index

import caseapp.RemainingArgs
import org.apache.hadoop.fs.FileSystem
import org.hammerlab.hadoop.Configuration
import org.hammerlab.test.Suite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.File

class IndexBlocksTest
  extends Suite {

  test("5k.bam") {
    val outPath = tmpPath()
    FileSystem.get(Configuration()).setVerifyChecksum(false)
    IndexBlocks.run(
      Args(
        outFile = Some(outPath.uri.toString)
      ),
      RemainingArgs(
        Seq(File("5k.bam")),
        Nil
      )
    )

    outPath should fileMatch("5k.bam.blocks")
  }
}
