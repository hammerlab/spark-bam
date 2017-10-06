package org.hammerlab.bam.benchmarks

import org.hammerlab.bytes.Bytes
import org.hammerlab.paths.Path

/**
 * synthesize spreadsheet rows by parsing stats from files output by `check-bam` and `check-blocks`
 */
object TSV {
  /**
   * Expects one argument: a path to a directory with a "bams" file listing paths to .bam files, as well as ".bam.out"
   * and ".bam.blocks.out" files for each BAM, with results from `check-bam` and `check-blocks`, resp.
   */
  def main(args: Array[String]): Unit = {
    val dataset = Dataset(Path(args(0)))
    val bamsPath = dataset.path / "bams"

    bamsPath
      .lines
      .foreach {
        bamStr ⇒
          val bam = Path("hdfs://" + bamStr)
          val name = bam.basename
          val data = BAM(bam, dataset)
          val compressedSize = data.compressedSize

          println(
            List(
              data.name,
              data.path,
              dataset.name,
              "",
              compressedSize,
              Bytes.format(compressedSize),
              data.uncompressedSize,
              data.reads,
              data.numFalsePositives,
              data.numFalseNegatives,
              data.numBlocks,
              data.badBlocks,
              data.badCompressedPositions
            )
            .mkString("\t")
          )
      }
  }
}
