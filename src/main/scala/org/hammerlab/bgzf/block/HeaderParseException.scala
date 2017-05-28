package org.hammerlab.bgzf.block

case class HeaderParseException(idx: Int,
                                actual: Byte,
                                expected: Byte)
  extends Exception(
    s"Position $idx: $actual != $expected"
  )
