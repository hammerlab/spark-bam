package org.hammerlab.bam.kryo

import java.util

import com.esotericsoftware.kryo.Kryo
import htsjdk.samtools.{ SAMFileHeader, SAMProgramRecord, SAMReadGroupRecord, SAMSequenceDictionary, SAMSequenceRecord }
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.bam.check
import org.hammerlab.bam.check.full.error.{ Counts, Flags }
import org.hammerlab.bam.check.{ Blocks, full, simple }
import org.hammerlab.bam.header.ContigLengths.ContigLengthsSerializer
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.bam.spark.{ Split, compare }
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.Metadata
import org.hammerlab.genomics.{ loci, reference }

import scala.collection.mutable

class Registrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    implicit val k = kryo
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[java.lang.Class[_]])

    kryo.register(check.TruePositive.getClass)
    kryo.register(check.TrueNegative.getClass)
    kryo.register(check.FalsePositive.getClass)
    kryo.register(check.FalseNegative.getClass)

    kryo.register(simple.TruePositive.getClass)
    kryo.register(simple.TrueNegative.getClass)
    kryo.register(simple.FalsePositive.getClass)
    kryo.register(simple.FalseNegative.getClass)

    kryo.register(full.TruePositive.getClass)
    kryo.register(classOf[full.TrueNegative])
    kryo.register(full.FalsePositive.getClass)
    kryo.register(classOf[full.FalseNegative])

    kryo.register(classOf[Flags])
    kryo.register(classOf[Counts])

    /**
     * [[org.hammerlab.bam.spark.LoadBamContext.loadBamIntervals()]] parallelizes a [[Vector]] of [[Vector]]s of
     * [[Chunk]]s
     */
    kryo.register(classOf[mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[Chunk])
    kryo.register(classOf[Pos])
    kryo.register(classOf[Array[Vector[_]]])

    /**
     * [[org.hammerlab.bam.spark.LoadBamContext.loadBam]] parallelizes an [[Array[Long]]] of file-split start-positions.
     */
    kryo.register(classOf[mutable.WrappedArray.ofLong])

    /** It also collects an [[Array[Split]]] in [[org.hammerlab.bam.spark.Spark]] mode */
    kryo.register(classOf[Array[Split]])
    kryo.register(classOf[Split])

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

    new reference.Registrar().registerClasses(kryo)

    /**
     * [[org.hammerlab.bam.spark.load.CanLoadBam.loadBamIntervals]] broadcasts a
     * [[org.hammerlab.genomics.loci.set.LociSet]]
     */
    new loci.set.Registrar().registerClasses(kryo)

    kryo.register(classOf[Metadata])
    kryo.register(classOf[Array[Metadata]])

    kryo.register(classOf[Pos])
    kryo.register(classOf[Array[Pos]])

    kryo.register(classOf[Header])

    compare.Main.register(kryo)

    Blocks.register
  }
}
