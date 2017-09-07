package org.hammerlab.bam.check.full.error

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.immutable.BitSet

class Registrar
  extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[BitSet])
    kryo.register(classOf[Flags], new FlagsSerializer)
  }
}
