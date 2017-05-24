package org.hammerlab.hadoop_bam.bgzf.block

case class Metadata(start: Long,
                    uncompressedSize: Int,
                    compressedSize: Int) {

}
