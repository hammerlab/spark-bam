package org.hammerlab.bgzf.block

case class Pointer(start: Long,
                   compressedSize: Int,
                   uncompressedSize: Int)

