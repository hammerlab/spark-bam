package org.hammerlab.hadoop_bam.bam

import org.apache.hadoop.fs.Path
import org.hammerlab.hadoop_bam.bgzf.Pos
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.resources.File
import org.hammerlab.test.matchers.seqs.SeqMatcher.seqMatch

class CheckBAMTest
  extends SparkSuite {

  test("check") {
    val calls =
      CheckBAM.run(
        sc,
        new Path(File("2.bam")),
        CheckBAMArgs(
          File("blocks"),
          File("records"),
          File("2.bam"),
          Some(100)
        )
      )

    val callHist =
      calls
        .values
        .map(_ → 1L)
        .reduceByKey(_ + _)
        .map(_.swap)
        .sortByKey(ascending = false)

    println(callHist.count)
    println(callHist.take(10))

//    callHist.count should be(294)
//    callHist.take(10) should be(
//      Array(
//        368834 →
//          TrueNegative(
//            PosAndVariableErrors(
//              RefPosErrors(
//                TooLargeRefIdx,
//                TooLargeRefIdx
//              ),
//              TooFewBytesForSeqAndQuals(
//                Some(NonNullTerminatedReadName),
//                Some(InvalidCigarOp)
//              )
//            )
//          ),
//        61809 →
//          TrueNegative(
//            PosAndVariableErrors(
//              RefPosErrors(
//                TooLargeRefIdx,
//                TooLargeRefIdx
//              ),
//              TooFewBytesForSeqAndQuals(
//                Some(NonASCIIReadName),
//                Some(InvalidCigarOp)
//              )
//            )
//          ),
//        49573 →
//          TrueNegative(
//            PosAndVariableErrors(
//              RefPosErrors(
//                TooLargeRefIdx,
//                TooLargeRefIdx
//              ),
//              ReadNameAndCigarOpsErrors(
//                NonNullTerminatedReadName,
//                InvalidCigarOp
//              )
//            )
//          ),
//        22177 →
//          TrueNegative(
//            PosAndVariableErrors(
//              RefPosErrors(
//                TooLargeRefIdx,
//                TooLargeRefIdx
//              ),
//              TooFewBytesForSeqAndQuals(
//                Some(NoReadName),
//                Some(InvalidCigarOp)
//              )
//            )
//          ),
//        15514 →
//          TrueNegative(
//            PosAndVariableErrors(
//              RefPosErrors(
//                NegativeRefIdx,
//                TooLargeRefIdx
//              ),
//              TooFewBytesForSeqAndQuals(
//                Some(NonNullTerminatedReadName),
//                Some(InvalidCigarOp)
//              )
//            )
//          ),
//        12929 →
//          TrueNegative(
//            PosAndVariableErrors(
//              RefPosErrors(
//                TooLargeRefIdxNegativePos,
//                TooLargeRefIdx
//              ),
//              TooFewBytesForSeqAndQuals(
//                Some(NonNullTerminatedReadName),
//                Some(InvalidCigarOp)
//              )
//            )
//          ),
//        10479 →
//          TrueNegative(
//            PosAndVariableErrors(
//              RefPosErrors(
//                TooLargeRefIdx,
//                TooLargeRefIdx
//              ),
//              ReadNameAndCigarOpsErrors(
//                NoReadName,
//                InvalidCigarOp
//              )
//            )
//          ),
//        9678 →
//          TrueNegative(
//            PosAndVariableErrors(
//              RefPosErrors(
//                TooLargeRefIdx,
//                TooLargeRefIdxNegativePos
//              ),
//              TooFewBytesForSeqAndQuals(
//                Some(NonNullTerminatedReadName),
//                Some(InvalidCigarOp)
//              )
//            )
//          ),
//        8844 →
//          TrueNegative(
//            PosAndVariableErrors(
//              RefPosErrors(
//                TooLargeRefIdx,
//                NegativeRefIdx
//              ),
//              TooFewBytesForSeqAndQuals(
//                Some(NonNullTerminatedReadName),
//                Some(InvalidCigarOp)
//              )
//            )
//          ),
//        7675 →
//          TrueNegative(
//            PosAndVariableErrors(
//              RefPosErrors(
//                TooLargeRefIdx,
//                TooLargeRefIdx
//              ),
//              TooFewBytesForSeqAndQuals(
//                Some(NoReadName),
//                None
//              )
//            )
//          )
//      )
//    )

    val falseCallsHist =
      callHist.filter {
        _._2 match {
          case f: False ⇒ true
          case _ ⇒ false
        }
      }

    falseCallsHist.count should be(0)
  }

//  test("fail") {
//    val errors =
//      CheckBAM.run(
//        sc,
//        new Path(File("2.bam")),
//        Args(
//          File("blocks"),
//          File("records"),
//          File("2.bam"),
//          blocksWhitelist = Some("268458108")
//        )
//      )
//      .collect
//
//    errors should be(
//      Array(
//        FalsePositive(
//          Pos(268458108, 115),
//          Pos(268458108, 116),
//          true
//        )
//      )
//    )
//  }
//
//  test("fail2") {
//    val errors =
//      CheckBAM.run(
//        sc,
//        new Path(File("2.bam")),
//        Args(
//          File("blocks"),
//          File("records"),
//          File("2.bam"),
//          blocksWhitelist = Some("201206175,215198728,248476903,253384328")
//        )
//      )
//      .collect
//
//    errors should be(
//      Array(
//        FalsePositive(Pos(201206175,  6940), Pos(201206175,  6941), true),
//        FalsePositive(Pos(215198728, 50073), Pos(215198728, 50074), true),
//        FalsePositive(Pos(248476903, 30794), Pos(248476903, 30795), true),
//        FalsePositive(Pos(253384328, 55615), Pos(253384328, 55616), true)
//      )
//    )
//  }

}
