package org.hammerlab.bam.rewrite

import caseapp.RemainingArgs
import org.hammerlab.args.{ Endpoints, IntRanges }
import org.hammerlab.bam.test.resources.bam5k
import org.hammerlab.test.Suite
import org.hammerlab.test.matchers.files.DirMatcher.dirMatch
import org.hammerlab.test.resources.File

class MainTest
  extends Suite {

  /**
   * Use [[Main]] to pull records [100,3000) out of 5k.bam, test that the results are as expected.
   */
  test("slice 5k.bam") {
    val outDir = tmpDir()
    Main.run(
      Args(
        readRanges =
          Some(
            IntRanges(
              Seq(
                Endpoints(100, 3000)
              )
            )
          ),
        indexBlocks = true,
        indexRecords = true
      ),
      RemainingArgs(
        Seq(
          bam5k.toString,
          s"$outDir/5k.100-3000.bam"
        ),
        Nil
      )
    )

    outDir should dirMatch(File("5k.100-3000"))
  }
}
