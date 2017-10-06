package org.hammerlab.bam.check.blocks

import org.hammerlab.bam.spark.MainSuite
import org.hammerlab.bam.test.resources.{ bam1, bam2, bam1BlockAligned }
import org.hammerlab.paths.Path

class MainTest
  extends MainSuite(Main) {

  override def defaultOpts(outPath: Path) = Seq("-o", outPath)

  test("1.bam") {
    check(
      bam1
    )(
      """First read-position mis-matched in 1 of 25 BGZF blocks
        |
        |25871 of 597482 (0.043300049206503294) compressed positions would lead to bad splits
        |
        |Offsets of blocks' first reads (0 blocks didn't contain a read start):
        |N: 25, μ/σ: 2004/8950, med/mad: 191/110
        | elems: 1 25 28 39 42 45 81 112 136 143 … 268 270 271 287 301 304 311 312 316 45846
        |   5:	8
        |  10:	27
        |  25:	63
        |  50:	191
        |  75:	294
        |  90:	314
        |  95:	32187
        |
        |1 mismatched blocks:
        |	239479 (prev block size: 25871):	239479:312	239479:311
        |"""
    )
  }

  test("1.bam spark-bam") {
    check(
      "-s",
      bam1
    )(
      """First read-position matched in 25 BGZF blocks totaling 583KB (compressed)
        |
        |Offsets of blocks' first reads (0 blocks didn't contain a read start):
        |N: 25, μ/σ: 2004/8950, med/mad: 191/110
        | elems: 1 25 28 39 42 45 81 112 136 143 … 268 270 271 287 301 304 311 312 316 45846
        |   5:	8
        |  10:	27
        |  25:	63
        |  50:	191
        |  75:	294
        |  90:	314
        |  95:	32187
        |"""
    )
  }

  test("1.bam hadoop-bam") {
    check(
      "-u",
      bam1
    )(
      """First read-position mis-matched in 1 of 25 BGZF blocks
        |
        |25871 of 597482 (0.043300049206503294) compressed positions would lead to bad splits
        |
        |Offsets of blocks' first reads (0 blocks didn't contain a read start):
        |N: 25, μ/σ: 2004/8950, med/mad: 191/110
        | elems: 1 25 28 39 42 45 81 112 136 143 … 268 270 271 287 301 304 311 312 316 45846
        |   5:	8
        |  10:	27
        |  25:	63
        |  50:	191
        |  75:	294
        |  90:	314
        |  95:	32187
        |
        |1 mismatched blocks:
        |	239479 (prev block size: 25871):	239479:312	239479:311
        |"""
    )
  }

  test("2.bam") {
    check(
      bam2
    )(
      """First read-position matched in 25 BGZF blocks totaling 519KB (compressed)
        |
        |Offsets of blocks' first reads (0 blocks didn't contain a read start):
        |N: 25, μ/σ: 604/1049, med/mad: 470/152
        | elems: 65 90 122 139 152 177 184 279 304 316 … 538 565 587 603 611×2 616 618 622 642 5650
        |   5:	73
        |  10:	109
        |  25:	181
        |  50:	470
        |  75:	611
        |  90:	630
        |  95:	4148
        |"""
    )
  }

  test("1.block-aligned.bam") {
    check(
      bam1BlockAligned
    )(
      """First read-position matched in 25 BGZF blocks totaling 575KB (compressed)
        |
        |24 blocks start with a read, 1 blocks didn't contain a read
        |"""
    )
  }
}
