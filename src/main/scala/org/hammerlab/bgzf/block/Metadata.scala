package org.hammerlab.bgzf.block

/**
 * BGZF-block metadata
 */
case class Metadata(start: Long,
                    uncompressedSize: Int,
                    compressedSize: Int)
