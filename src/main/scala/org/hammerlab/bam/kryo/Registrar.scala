package org.hammerlab.bam.kryo

import java.util

import com.esotericsoftware.kryo.Kryo
import htsjdk.samtools.{ SAMFileHeader, SAMProgramRecord, SAMReadGroupRecord, SAMSequenceDictionary, SAMSequenceRecord }
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.bam.header.ContigLengths.ContigLengthsSerializer
import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.bgzf.Pos
import org.hammerlab.hadoop

import scala.collection.mutable

object Registrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    hadoop.Registrar.registerClasses(kryo)

    /** [[org.hammerlab.bam.hadoop.LoadBam.LoadBamContext]] parallelizes a [[Vector]] of [[Vector]]s of [[Chunk]]s */
    kryo.register(classOf[mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[Chunk])
    kryo.register(classOf[Pos])
    kryo.register(classOf[Array[Vector[_]]])

    /** [[SAMFileHeader]] */
    kryo.register(classOf[SAMFileHeader])
    kryo.register(classOf[util.LinkedHashMap[_, _]])
    kryo.register(classOf[util.ArrayList[_]])
    kryo.register(classOf[util.HashMap[_, _]])
    kryo.register(classOf[SAMReadGroupRecord])
    kryo.register(classOf[SAMSequenceDictionary])
    kryo.register(classOf[SAMSequenceRecord])
    kryo.register(classOf[SAMProgramRecord])

    /** Backs [[org.hammerlab.bam.header.ContigLengths]] */
    kryo.register(classOf[ContigLengths], ContigLengthsSerializer)
  }
}
