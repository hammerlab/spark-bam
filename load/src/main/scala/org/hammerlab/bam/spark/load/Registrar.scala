package org.hammerlab.bam.spark.load

import htsjdk.samtools.SAMFileHeader
import org.hammerlab.bam.header.{ ContigLengths, Header }
import org.hammerlab.bam.index.Index.Chunk
import org.hammerlab.bam.kryo.registerSAMFileHeader
import org.hammerlab.bgzf.Pos
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.hadoop.Configuration
import org.hammerlab.kryo._

import scala.collection.mutable

case class Registrar()
  extends spark.Registrar(

        /** Several [[CanLoadBam]] methods broadcast [[Header]] and/or [[ContigLengths]] */
        cls[Header],
        cls[ContigLengths],
        cls[Configuration],

        /** [[CanLoadBam.loadSam]] broadcasts a [[htsjdk.samtools.SAMFileHeader]] */
        cls[SAMFileHeader],

        /**
         * An [[org.apache.spark.rdd.RDD]] of [[Pos]] is [[org.apache.spark.rdd.RDD.collect collect]]ed in
         * [[CanLoadBam.loadSplitsAndReads]]
         */
        cls[mutable.WrappedArray.ofRef[_]],
        arr[Pos],

        /** [[CanLoadBam.loadBamIntervals]] broadcasts a [[org.hammerlab.genomics.loci.set.LociSet]] */
        cls[LociSet],

        /** [[CanLoadBam.loadBamIntervals]] [[org.apache.spark.SparkContext.parallelize parallelize]]s some [[Vector]]s */
        arr[Vector[_]],
        cls[Chunk]
  )
