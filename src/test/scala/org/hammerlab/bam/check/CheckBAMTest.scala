package org.hammerlab.bam.check

import org.hammerlab.bam.check.Error.Counts
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File

class CheckBAMTest
  extends SparkSuite {

  def check(args: Args,
            expectedNumPositions: Int,
            expectedFlagCounts: Counts): Unit = {
    val Result(
      numPositions,
      calls,
      numFalseCalls,
      falseCalls,
      criticalErrorCounts,
      totalErrorCounts,
      _
    ) =
      Main.run(sc, args)

    numPositions should be(expectedNumPositions)

    if (numFalseCalls > 0) {
      println(falseCalls.take(10).mkString("\n"))
    }
    numFalseCalls should be(0)

    totalErrorCounts should be(expectedFlagCounts)
  }

  test("5k.bam header block") {
    check(
      Args(
        File("5k.bam"),
        numBlocks = Some(1)
      ),
      5650,
      ErrorT[Long](
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
        File("5k.bam"),
        blocksWhitelist = Some("27784")
      ),
      64902,
      ErrorT[Long](
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
        File("5k.bam"),
        numBlocks = Some(10)
      ),
      590166,
      ErrorT[Long](
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
      Args(
        File("5k.bam")
      ),
      3139404,
      ErrorT[Long](
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
