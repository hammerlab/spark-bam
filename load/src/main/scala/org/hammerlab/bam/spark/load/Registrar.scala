package org.hammerlab.bam.spark.load

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.bam
import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.genomics.loci

class Registrar
  extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {

    bam.kryo.Registrar.registerClasses(kryo)

    /** [[CanLoadBam.loadBamIntervals]] broadcasts a [[org.hammerlab.genomics.loci.set.LociSet]] */
    new loci.set.Registrar().registerClasses(kryo)

    /** [[CanLoadBam.loadBamIntervals]] [[org.apache.spark.SparkContext.parallelize parallelize]]s some [[Vector]]s */
    kryo.register(classOf[Array[Vector[_]]])
    kryo.register(classOf[Chunk])
  }
}
