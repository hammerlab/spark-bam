package org.hammerlab.io

import java.io.InputStream

case class ByteStream(it: Iterator[Byte])
  extends InputStream {
  override def read(): Int = it.next
}
