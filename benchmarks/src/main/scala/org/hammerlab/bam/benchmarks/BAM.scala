package org.hammerlab.bam.benchmarks

import hammerlab.path._

case class Datasets(datasets: Map[Dataset, Seq[BAM]])

object Datasets {
  def apply(root: Path,
            bamScheme: String): Datasets =
    apply(root, Some(bamScheme))

  def apply(root: Path,
            bamScheme: Option[String] = None): Datasets =
    Datasets(
      root
        .walk
        .filter(_.basename == "bams")
        .map {
          bamsPath ⇒
            val dataset = Dataset(bamsPath.parent)
            val bams =
              bamsPath
                .lines
                .map(
                  bam ⇒
                    bamScheme
                      .map(scheme ⇒ s"$scheme://$bam")
                      .getOrElse(bam)
                )
                .map(Path(_))
                .map(BAM(_, dataset))

            dataset → bams.toVector
        }
        .toMap
    )
}

case class Dataset(path: Path) {
  def name = path.basename
}

case class BlocksInfo(numBlocks: Long,
                      badBlocks: Long,
                      compressedSize: Long,
                      badCompressedPositions: Long)

object BlocksInfo {

  val mismatched = """First read-position mis-matched in (\d+) of (\d+) BGZF blocks.*""".r
  val badCompressedPositionsRegex = """^(\d+) of (\d+) .*""".r
  val matched = """First read-position matched in (\d+) BGZF blocks .*""".r

  def apply(path: Path, compressedSize: Long): BlocksInfo = {
    val lines = path.lines
    val header = lines.next
    header match {
      case mismatched(badBlocks, numBlocks) ⇒
        lines
          .collect {
            case badCompressedPositionsRegex(badCompressedPositions, compressedSize2) ⇒
              (badCompressedPositions.toLong, compressedSize2.toLong)
          }
          .next match {
          case (badCompressedPositions, compressedSize2) ⇒
            if (compressedSize != compressedSize2)
              throw new IllegalStateException(
                s"Supplied file-size $compressedSize doesn't match .blocks.out value $compressedSize2"
              )

            BlocksInfo(
              numBlocks.toLong,
              badBlocks.toLong,
              compressedSize,
              badCompressedPositions
            )
        }
      case matched(numBlocks) ⇒
        BlocksInfo(
          numBlocks.toLong,
          0,
          compressedSize,
          0
        )
    }
  }
}

case class BAMInfo(uncompressedPositions: Long,
                   reads: Long,
                   falsePositives: Long,
                   falseNegatives: Long)

object BAMInfo {

  val uncompressedPositionsRegex = """(\d+) uncompressed positions""".r
  val readsRegex = """(\d+) reads""".r
  val falseCallsRegex = """(\d+) false positives, (\d+) false negatives""".r

  def apply(path: Path): BAMInfo = {
    val lines = path.lines.take(5).toVector
    (lines(0), lines(3))
    val uncompressedPositions =
      lines(0) match {
        case uncompressedPositionsRegex(n) ⇒ n.toLong
      }

    val reads =
      lines(3) match {
        case readsRegex(n) ⇒ n.toLong
      }

    val (falsePositives, falseNegatives) =
      lines(4) match {
        case falseCallsRegex(fp, fn) ⇒ (fp.toLong, fn.toLong)
        case "All calls matched!" ⇒ (0L, 0L)
      }

    BAMInfo(
      uncompressedPositions,
      reads,
      falsePositives,
      falseNegatives
    )
  }
}

case class BAM(name: String,
               path: Path,
               dataset: Dataset,
               compressedSize: Long,
               badCompressedPositions: Long,
               uncompressedSize: Long,
               reads: Long,
               numBlocks: Long,
               badBlocks: Long,
               numFalsePositives: Long,
               numFalseNegatives: Long) {
}

object BAM {
  def apply(path: Path, dataset: Dataset): BAM = {
    val name = path.basename
    val basePath = dataset.path / name
    val checkBamOut = basePath + ".out"
    val checkBlocksOut = basePath + ".blocks.out"
    val compressedSize = path.size
    val BlocksInfo(numBlocks, badBlocks, _, badCompressedPositions) = BlocksInfo(checkBlocksOut, compressedSize)
    val BAMInfo(uncompressedPositions, reads, falsePositives, falseNegatives) = BAMInfo(checkBamOut)

    BAM(
      name = name,
      path = path,
      dataset = dataset,
      compressedSize = compressedSize,
      badCompressedPositions = badCompressedPositions,
      uncompressedSize = uncompressedPositions,
      reads = reads,
      numBlocks = numBlocks,
      badBlocks = badBlocks,
      numFalsePositives = falsePositives,
      numFalseNegatives = falseNegatives
    )
  }

  import cats.Show.show

/*
  val showTSV: Show[BAM] =
    show {
      case BAM(
        name,
        path,
        dataset,
        compressedSize,
        badCompressedPositions,
        uncompressedSize,
        numBlocks,
        badBlocks,
        numFalsePositives,
        numFalseNegatives
      ) ⇒
      List(
        name,
        path,
        dataset,
        "",

      )
    }
*/
}
