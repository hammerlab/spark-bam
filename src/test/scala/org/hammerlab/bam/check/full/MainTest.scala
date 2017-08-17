package org.hammerlab.bam.check.full

import org.hammerlab.resources.{ bam5k, tcgaBamExcerpt, tcgaBamExcerptUnindexed }
import org.hammerlab.spark.test.suite.MainSuite

class MainTest
  extends MainSuite {

  implicit def wrapOpt[T](t: T): Option[T] = Some(t)

  val expectedTcgaHeader =
    """2580596 uncompressed positions
      |941K compressed
      |Compression ratio: 2.68
      |7976 reads
      |All calls matched!
      |"""
    .stripMargin

  val expectedTcgaFlagStats =
    """No positions where only one check failed
      |
      |10 of 1208 positions where exactly two checks failed:
      |	14146:8327:	296 before D0N7FACXX120305:2:2107:10453:199544 1/2 76b aligned read @ 1:24795498. Failing checks: tooLargeNextReadIdx,invalidCigarOp
      |	39374:60226:	2 before C0FR5ACXX120302:8:1303:16442:188614 2/2 76b aligned read @ 1:24838240. Failing checks: nonNullTerminatedReadName,invalidCigarOp
      |	39374:60544:	2 before C0FR5ACXX120302:8:1303:16442:188614 1/2 76b aligned read @ 1:24838417. Failing checks: nonNullTerminatedReadName,invalidCigarOp
      |	39374:60862:	2 before C0FR5ACXX120302:3:1203:11738:62429 2/2 76b aligned read @ 1:24840504. Failing checks: nonNullTerminatedReadName,invalidCigarOp
      |	39374:61179:	2 before C0FR5ACXX120302:3:1203:11758:62415 2/2 76b aligned read @ 1:24840504. Failing checks: nonNullTerminatedReadName,invalidCigarOp
      |	39374:61496:	2 before C0FR5ACXX120302:3:2206:18762:165472 1/2 76b aligned read @ 1:24840609. Failing checks: nonNullTerminatedReadName,invalidCigarOp
      |	39374:61814:	2 before C0FR5ACXX120302:4:1107:12894:63952 2/2 76b aligned read @ 1:24840639. Failing checks: nonNullTerminatedReadName,invalidCigarOp
      |	39374:62131:	2 before D0N7FACXX120305:6:1101:10376:28505 2/2 76b aligned read @ 1:24840639. Failing checks: nonNullTerminatedReadName,invalidCigarOp
      |	39374:62448:	2 before D0N7FACXX120305:3:1102:11046:29695 2/2 76b aligned read @ 1:24840667. Failing checks: nonNullTerminatedReadName,invalidCigarOp
      |	39374:62765:	2 before C0FR5ACXX120302:2:2301:2419:18406 2/2 76b aligned read @ 1:24840675. Failing checks: nonNullTerminatedReadName,invalidCigarOp
      |	…
      |
      |	Histogram:
      |		1193:	nonNullTerminatedReadName,invalidCigarOp
      |		15:	tooLargeNextReadIdx,invalidCigarOp
      |
      |	Per-flag totals:
      |	             invalidCigarOp:	1208
      |	  nonNullTerminatedReadName:	1193
      |	        tooLargeNextReadIdx:	  15
      |
      |Total error counts:
      |             invalidCigarOp:	2454770
      |        tooLargeNextReadIdx:	2391574
      |            tooLargeReadIdx:	2391574
      |  nonNullTerminatedReadName:	2113790
      |tooFewRemainingBytesImplied:	1965625
      |           nonASCIIReadName:	 221393
      |                 noReadName:	 202320
      |        negativeNextReadIdx:	 116369
      |            negativeReadIdx:	 116369
      |            negativeReadPos:	 116369
      |        negativeNextReadPos:	 116369
      |        tooLargeNextReadPos:	  37496
      |            tooLargeReadPos:	  37496
      |              emptyReadName:	  32276
      |     tooFewBytesForReadName:	     77
      |     tooFewBytesForCigarOps:	      9
      |
      |"""
    .stripMargin

  def check(
      path: String
  )(
      args: String*
  )(
      expected: String
  ): Unit = {
    val outputPath = tmpPath()

    Main.main(
      args.toArray ++
        Array(
          "-l", "10",
          "-o", outputPath.toString,
          path
        )
    )

    outputPath.read should be(expected.stripMargin)
  }

  test("tcga excerpt with indexed records") {
    check(
      tcgaBamExcerpt
    )(
      "-m", "200k"
    )(
      List(
        expectedTcgaHeader,
        expectedTcgaFlagStats
      )
      .mkString("\n")
    )
  }

  test("tcga excerpt without indexed records") {
    check(
      tcgaBamExcerptUnindexed
    )(
      "-m", "200k"
    )(
      expectedTcgaFlagStats
    )
  }

  test("5k.bam header block") {
    check(
      bam5k
    )(
      "-i", "0"
    )(
      """5650 uncompressed positions
        |2.4K compressed
        |Compression ratio: 2.30
        |0 reads
        |All calls matched!
        |
        |No positions where only one check failed
        |
        |1 positions where exactly two checks failed:
        |	0:5649:	1 before HWI-ST807:461:C2P0JACXX:4:2115:8592:79724 2/2 101b aligned read @ 1:10001. Failing checks: noReadName,invalidCigarOp
        |
        |	Per-flag totals:
        |	                 noReadName:	1
        |	             invalidCigarOp:	1
        |
        |Total error counts:
        |            tooLargeReadIdx:	5457
        |        tooLargeNextReadIdx:	5452
        |             invalidCigarOp:	5402
        |  nonNullTerminatedReadName:	4829
        |tooFewRemainingBytesImplied:	3203
        |                 noReadName:	 438
        |           nonASCIIReadName:	 368
        |        negativeNextReadIdx:	 107
        |        negativeNextReadPos:	 107
        |            negativeReadIdx:	 106
        |            negativeReadPos:	 106
        |        tooLargeNextReadPos:	  76
        |            tooLargeReadPos:	  74
        |              emptyReadName:	   7
        |     tooFewBytesForReadName:	   0
        |     tooFewBytesForCigarOps:	   0
        |
        |"""
    )
  }

  test("5k.bam second block, with reads") {
    check(
      bam5k
    )(
      "-i", "27784"
    )(
      """64902 uncompressed positions
        |23.0K compressed
        |Compression ratio: 2.75
        |101 reads
        |All calls matched!
        |
        |No positions where only one check failed
        |
        |10 of 103 positions where exactly two checks failed:
        |	27784:663:	1 before HWI-ST807:461:C2P0JACXX:4:2302:19124:35915 1/2 101b aligned read @ 1:12682. Failing checks: noReadName,invalidCigarOp
        |	27784:1308:	1 before HWI-ST807:461:C2P0JACXX:4:2113:6067:5715 2/2 101b aligned read @ 1:12718. Failing checks: noReadName,invalidCigarOp
        |	27784:1991:	1 before HWI-ST807:461:C2P0JACXX:4:2115:18499:5086 2/2 101b aligned read @ 1:12718. Failing checks: noReadName,invalidCigarOp
        |	27784:2673:	1 before HWI-ST807:461:C2P0JACXX:4:1305:3329:11177 1/2 101b aligned read @ 1:12752. Failing checks: noReadName,invalidCigarOp
        |	27784:3319:	1 before HWI-ST807:461:C2P0JACXX:4:1205:18554:47682 2/2 101b aligned read @ 1:12761. Failing checks: noReadName,invalidCigarOp
        |	27784:3964:	1 before HWI-ST807:461:C2P0JACXX:4:1211:12710:58551 2/2 101b aligned read @ 1:12762. Failing checks: noReadName,invalidCigarOp
        |	27784:4610:	1 before HWI-ST807:461:C2P0JACXX:4:2103:14084:46618 2/2 101b aligned read @ 1:12783. Failing checks: noReadName,invalidCigarOp
        |	27784:5274:	1 before HWI-ST807:461:C2P0JACXX:4:1106:7594:14995 1/2 101b aligned read @ 1:12797. Failing checks: noReadName,invalidCigarOp
        |	27784:5918:	1 before HWI-ST807:461:C2P0JACXX:4:1307:14114:38761 1/2 101b aligned read @ 1:12807. Failing checks: noReadName,invalidCigarOp
        |	27784:6563:	1 before HWI-ST807:461:C2P0JACXX:4:2210:6972:43830 2/2 101b aligned read @ 1:12811. Failing checks: noReadName,invalidCigarOp
        |	…
        |
        |	Histogram:
        |		101:	noReadName,invalidCigarOp
        |		2:	nonNullTerminatedReadName,invalidCigarOp
        |
        |	Per-flag totals:
        |	             invalidCigarOp:	103
        |	                 noReadName:	101
        |	  nonNullTerminatedReadName:	  2
        |
        |Total error counts:
        |             invalidCigarOp:	63101
        |        tooLargeNextReadIdx:	62661
        |            tooLargeReadIdx:	62661
        |  nonNullTerminatedReadName:	56947
        |tooFewRemainingBytesImplied:	54837
        |           nonASCIIReadName:	 3820
        |                 noReadName:	 3696
        |        negativeNextReadIdx:	 1425
        |            negativeReadIdx:	 1425
        |            negativeReadPos:	 1424
        |        negativeNextReadPos:	 1424
        |        tooLargeNextReadPos:	  290
        |            tooLargeReadPos:	  289
        |              emptyReadName:	  208
        |     tooFewBytesForReadName:	    0
        |     tooFewBytesForCigarOps:	    0
        |
        |"""
    )
  }

  test("5k.bam 200k") {
    check(
      bam5k
    )(
      "-i", "-200k",
      "-m", "100k"
    )(
      """655132 uncompressed positions
        |215K compressed
        |Compression ratio: 2.98
        |1022 reads
        |All calls matched!
        |
        |No positions where only one check failed
        |
        |10 of 1023 positions where exactly two checks failed:
        |	0:5649:	1 before HWI-ST807:461:C2P0JACXX:4:2115:8592:79724 2/2 101b aligned read @ 1:10001. Failing checks: noReadName,invalidCigarOp
        |	2454:623:	1 before HWI-ST807:461:C2P0JACXX:4:2115:8592:79724 1/2 101b aligned read @ 1:10009. Failing checks: noReadName,invalidCigarOp
        |	2454:1243:	1 before HWI-ST807:461:C2P0JACXX:4:1304:9505:89866 2/2 101b aligned read @ 1:10048. Failing checks: noReadName,invalidCigarOp
        |	2454:1882:	1 before HWI-ST807:461:C2P0JACXX:4:2311:6431:65669 2/2 101b aligned read @ 1:10335. Failing checks: noReadName,invalidCigarOp
        |	2454:2519:	1 before HWI-ST807:461:C2P0JACXX:4:1305:2342:51860 1/2 101b unmapped read (placed at 1:10363). Failing checks: noReadName,invalidCigarOp
        |	2454:3087:	1 before HWI-ST807:461:C2P0JACXX:4:1305:2342:51860 2/2 101b aligned read @ 1:10363. Failing checks: noReadName,invalidCigarOp
        |	2454:3733:	1 before HWI-ST807:461:C2P0JACXX:4:1304:9505:89866 1/2 101b aligned read @ 1:10368. Failing checks: noReadName,invalidCigarOp
        |	2454:4367:	1 before HWI-ST807:461:C2P0JACXX:4:2311:6431:65669 1/2 101b aligned read @ 1:10458. Failing checks: noReadName,invalidCigarOp
        |	2454:4986:	1 before HWI-ST807:461:C2P0JACXX:4:1107:13461:64844 1/2 101b aligned read @ 1:11648. Failing checks: noReadName,invalidCigarOp
        |	2454:5667:	1 before HWI-ST807:461:C2P0JACXX:4:2203:17157:59976 2/2 101b aligned read @ 1:11687. Failing checks: noReadName,invalidCigarOp
        |	…
        |
        |	Histogram:
        |		1020:	noReadName,invalidCigarOp
        |		3:	nonNullTerminatedReadName,invalidCigarOp
        |
        |	Per-flag totals:
        |	             invalidCigarOp:	1023
        |	                 noReadName:	1020
        |	  nonNullTerminatedReadName:	   3
        |
        |Total error counts:
        |             invalidCigarOp:	636643
        |            tooLargeReadIdx:	632450
        |        tooLargeNextReadIdx:	632444
        |  nonNullTerminatedReadName:	574675
        |tooFewRemainingBytesImplied:	545259
        |           nonASCIIReadName:	 38387
        |                 noReadName:	 37336
        |        negativeNextReadIdx:	 14144
        |        negativeNextReadPos:	 14144
        |            negativeReadPos:	 14143
        |            negativeReadIdx:	 14142
        |        tooLargeNextReadPos:	  3253
        |            tooLargeReadPos:	  3251
        |              emptyReadName:	  2112
        |     tooFewBytesForReadName:	     0
        |     tooFewBytesForCigarOps:	     0
        |
        |"""
    )
  }

  test("5k.bam all") {
    check(
      bam5k
    )(
    )(
      """3139404 uncompressed positions
        |987K compressed
        |Compression ratio: 3.11
        |4910 reads
        |All calls matched!
        |
        |No positions where only one check failed
        |
        |10 of 5284 positions where exactly two checks failed:
        |	0:5649:	1 before HWI-ST807:461:C2P0JACXX:4:2115:8592:79724 2/2 101b aligned read @ 1:10001. Failing checks: noReadName,invalidCigarOp
        |	2454:623:	1 before HWI-ST807:461:C2P0JACXX:4:2115:8592:79724 1/2 101b aligned read @ 1:10009. Failing checks: noReadName,invalidCigarOp
        |	2454:1243:	1 before HWI-ST807:461:C2P0JACXX:4:1304:9505:89866 2/2 101b aligned read @ 1:10048. Failing checks: noReadName,invalidCigarOp
        |	2454:1882:	1 before HWI-ST807:461:C2P0JACXX:4:2311:6431:65669 2/2 101b aligned read @ 1:10335. Failing checks: noReadName,invalidCigarOp
        |	2454:2519:	1 before HWI-ST807:461:C2P0JACXX:4:1305:2342:51860 1/2 101b unmapped read (placed at 1:10363). Failing checks: noReadName,invalidCigarOp
        |	2454:3087:	1 before HWI-ST807:461:C2P0JACXX:4:1305:2342:51860 2/2 101b aligned read @ 1:10363. Failing checks: noReadName,invalidCigarOp
        |	2454:3733:	1 before HWI-ST807:461:C2P0JACXX:4:1304:9505:89866 1/2 101b aligned read @ 1:10368. Failing checks: noReadName,invalidCigarOp
        |	2454:4367:	1 before HWI-ST807:461:C2P0JACXX:4:2311:6431:65669 1/2 101b aligned read @ 1:10458. Failing checks: noReadName,invalidCigarOp
        |	2454:4986:	1 before HWI-ST807:461:C2P0JACXX:4:1107:13461:64844 1/2 101b aligned read @ 1:11648. Failing checks: noReadName,invalidCigarOp
        |	2454:5667:	1 before HWI-ST807:461:C2P0JACXX:4:2203:17157:59976 2/2 101b aligned read @ 1:11687. Failing checks: noReadName,invalidCigarOp
        |	…
        |
        |	Histogram:
        |		4880:	noReadName,invalidCigarOp
        |		378:	emptyReadName,invalidCigarOp
        |		25:	nonNullTerminatedReadName,invalidCigarOp
        |		1:	tooLargeReadIdx,nonNullTerminatedReadName
        |
        |	Per-flag totals:
        |	             invalidCigarOp:	5283
        |	                 noReadName:	4880
        |	              emptyReadName:	 378
        |	  nonNullTerminatedReadName:	  26
        |	            tooLargeReadIdx:	   1
        |
        |Total error counts:
        |             invalidCigarOp:	3051420
        |        tooLargeNextReadIdx:	3018629
        |            tooLargeReadIdx:	3018629
        |  nonNullTerminatedReadName:	2754305
        |tooFewRemainingBytesImplied:	2640873
        |           nonASCIIReadName:	 181648
        |                 noReadName:	 179963
        |        negativeNextReadIdx:	  80277
        |            negativeReadIdx:	  80277
        |            negativeReadPos:	  80277
        |        negativeNextReadPos:	  80277
        |        tooLargeNextReadPos:	  13099
        |            tooLargeReadPos:	  13099
        |              emptyReadName:	  10402
        |     tooFewBytesForReadName:	     70
        |     tooFewBytesForCigarOps:	     22
        |
        |"""
    )
  }
}
