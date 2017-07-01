package org.hammerlab.bam.check.full

import org.hammerlab.bam.check.Args
import org.hammerlab.bam.check.full.error.Counts
import org.hammerlab.paths.Path
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File

class RunTest
  extends SparkSuite {

  def check(args: Args,
            expectedNumPositions: Int,
            expectedNumReadStarts: Int,
            expectedFlagCounts: Counts)(
      implicit path: Path
  ): Unit = {
    val Result(
      numPositions,
      _,
      numFalseCalls,
      falseCalls,
      numReadStarts,
      _,
      _,
      totalErrorCounts,
      _
    ) =
      Run(args)

    numPositions should be(expectedNumPositions)

    numFalseCalls should be(0)

    totalErrorCounts should be(expectedFlagCounts)
  }

  {
    implicit val path = File("5k.bam").path

    test("5k.bam header block") {
      check(
        Args(
          numBlocks = Some(1)
        ),
        5650,
        0,
        Counts(
                      tooLargeReadIdx = 5457,
                  tooLargeNextReadIdx = 5452,
                       invalidCigarOp = 5402,
            nonNullTerminatedReadName = 4829,
          tooFewRemainingBytesImplied = 3203,
                           noReadName =  438,
                     nonASCIIReadName =  368,
                  negativeNextReadIdx =  107,
                  negativeNextReadPos =  107,
                      negativeReadIdx =  106,
                      negativeReadPos =  106,
                  tooLargeNextReadPos =   76,
                      tooLargeReadPos =   74,
                        emptyReadName =    7,
               tooFewBytesForReadName =    0,
               tooFewBytesForCigarOps =    0,
                tooFewFixedBlockBytes =    0
        )
      )
    }

    test("5k.bam 2nd block of reads") {
      check(
        Args(
          blocksWhitelist = Some("27784")
        ),
        64902,
        101,
        Counts(
                       invalidCigarOp = 63101,
                  tooLargeNextReadIdx = 62661,
                      tooLargeReadIdx = 62661,
            nonNullTerminatedReadName = 56947,
          tooFewRemainingBytesImplied = 54837,
                     nonASCIIReadName =  3820,
                           noReadName =  3696,
                  negativeNextReadIdx =  1425,
                      negativeReadIdx =  1425,
                      negativeReadPos =  1424,
                  negativeNextReadPos =  1424,
                  tooLargeNextReadPos =   290,
                      tooLargeReadPos =   289,
                        emptyReadName =   208,
               tooFewBytesForReadName =     0,
               tooFewBytesForCigarOps =     0,
                tooFewFixedBlockBytes =     0
        )
      )
    }

    test("5k.bam 10 blocks") {
      check(
        Args(
          numBlocks = Some(10)
        ),
        590166,
        918,
        Counts(
                       invalidCigarOp = 573463,
                      tooLargeReadIdx = 569630,
                  tooLargeNextReadIdx = 569624,
            nonNullTerminatedReadName = 517552,
          tooFewRemainingBytesImplied = 490690,
                     nonASCIIReadName =  34590,
                           noReadName =  33701,
                  negativeNextReadIdx =  12847,
                  negativeNextReadPos =  12847,
                      negativeReadPos =  12846,
                      negativeReadIdx =  12845,
                  tooLargeNextReadPos =   2963,
                      tooLargeReadPos =   2962,
                        emptyReadName =   1938,
               tooFewBytesForReadName =      0,
               tooFewBytesForCigarOps =      0,
                tooFewFixedBlockBytes =      0
        )
      )
    }

    test("5k.bam all") {
      check(
        Args(),
        3139404,
        4910,
        Counts(
                       invalidCigarOp = 3051420,
                  tooLargeNextReadIdx = 3018629,
                      tooLargeReadIdx = 3018629,
            nonNullTerminatedReadName = 2754305,
          tooFewRemainingBytesImplied = 2640873,
                     nonASCIIReadName =  181648,
                           noReadName =  179963,
                  negativeNextReadIdx =   80277,
                      negativeReadIdx =   80277,
                      negativeReadPos =   80277,
                  negativeNextReadPos =   80277,
                  tooLargeNextReadPos =   13099,
                      tooLargeReadPos =   13099,
                        emptyReadName =   10402,
               tooFewBytesForReadName =      70,
                tooFewFixedBlockBytes =      35,
               tooFewBytesForCigarOps =      22
        )
      )
    }
}
}
