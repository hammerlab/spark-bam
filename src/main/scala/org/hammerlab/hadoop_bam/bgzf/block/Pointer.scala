package org.hammerlab.hadoop_bam.bgzf.block

case class Pointer(start: Long,
                   compressedSize: Int,
                   uncompressedSize: Int)

