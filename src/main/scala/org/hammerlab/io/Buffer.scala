package org.hammerlab.io

import java.nio.ByteOrder.LITTLE_ENDIAN
import java.nio.{ ByteBuffer, ByteOrder }

object Buffer {
  def apply(capacity: Int, order: ByteOrder = LITTLE_ENDIAN): ByteBuffer =
    ByteBuffer
      .allocate(capacity)
      .order(LITTLE_ENDIAN)
}
