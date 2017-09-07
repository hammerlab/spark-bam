package org.hammerlab.bam.check.full.error

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }

import scala.collection.immutable.BitSet

/**
 * Serialize a [[Flags]] as a [[BitSet]] to save IO.
 */
class FlagsSerializer
  extends Serializer[Flags] {
  override def read(kryo: Kryo, input: Input, clz: Class[Flags]): Flags = {
    kryo
      .readClassAndObject(input)
      .asInstanceOf[(BitSet, Int)]
  }

  override def write(kryo: Kryo, output: Output, flags: Flags): Unit =
    kryo.writeClassAndObject(output, flags: (BitSet, Int))
}
