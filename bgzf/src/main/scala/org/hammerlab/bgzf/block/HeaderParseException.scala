package org.hammerlab.bgzf.block

/**
 * Exception indicating that a bgzf-header magic-byte was not as expected
 */
case class HeaderParseException(idx: Int,
                                actual: Byte,
                                expected: Byte)
  extends Exception(
    s"Position $idx: $actual != $expected"
  )
