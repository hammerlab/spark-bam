package org.hammerlab.bam.rewrite

import org.hammerlab.bam.test.resources.bam2
import org.hammerlab.cli.base.app.MainSuite
import org.hammerlab.test.resources.File

class HTSJDKRewriteTest
  extends MainSuite(HTSJDKRewrite) {

  /**
   * Use [[HTSJDKRewrite]] to pull records [100,1000) out of 2.bam, test that the results are as expected.
   */
  test("slice 2.bam") {
    run(
      Seq(
        "-b",              // index blocks
        "-i",              // index records
        "-r", "100-1000",  // select reads with indices ∈ [100,1000)
        bam2
      )
    )
    .parent should dirMatch(File("slice"))
  }

  override def outBasename = "2.100-1000.bam"
}
