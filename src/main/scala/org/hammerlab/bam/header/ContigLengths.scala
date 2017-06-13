package org.hammerlab.bam.header

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import htsjdk.samtools.{ SAMFileHeader, SAMSequenceDictionary, SAMSequenceRecord }
import org.apache.hadoop.conf.Configuration
import org.hammerlab.bam.header.ContigLengths.ContigLengthsT
import org.hammerlab.genomics.reference.{ ContigName, NumLoci }
import org.hammerlab.hadoop.Path
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

case class ContigLengths(map: ContigLengthsT)
  extends Serializable {
  override def toString: String =
    (for {
      (_, (name, length)) ← map
    } yield
      s"$name:\t$length"
    )
    .mkString("\n")
}

object ContigLengths {

  type ContigLengthsT = SortedMap[Int, (ContigName, NumLoci)]

  def apply(path: Path)(implicit conf: Configuration): ContigLengths =
    SAMHeaderReader.readSAMHeaderFrom(path, conf)

  def apply(sequences: Seq[(Int, (ContigName, NumLoci))]): ContigLengths =
    ContigLengths(
      SortedMap(
        sequences: _*
      )
    )

  implicit def wrapContigLengths(contigLengths: ContigLengthsT): ContigLengths = ContigLengths(contigLengths)
  implicit def unwrapContigLengths(contigLengths: ContigLengths): ContigLengthsT = contigLengths.map

  implicit def htsjdkHeaderToContigLengths(contigLengths: ContigLengths): SAMFileHeader =
    new SAMFileHeader(
      new SAMSequenceDictionary(
        (for {
          (_, (name, length)) ← contigLengths.toList
        } yield
          new SAMSequenceRecord(name.name, length.toInt)
        )
        .asJava
      )
    )
  implicit def htsjdkHeaderToContigLengths(header: SAMFileHeader): ContigLengths =
    ContigLengths(
      for {
        sequence ← header.getSequenceDictionary.getSequences.asScala
      } yield
        sequence.getSequenceIndex →
          (
            sequence.getSequenceName: ContigName,
            NumLoci(sequence.getSequenceLength)
          )
    )

  implicit object ContigLengthsSerializer extends Serializer[ContigLengths] {
    override def write(kryo: Kryo, output: Output, contigLengths: ContigLengths): Unit = {
      output.writeInt(contigLengths.size)
      for {
        (_, (name, length)) ← contigLengths
      } {
        kryo.writeClassAndObject(output, name)
        output.writeLong(length)
      }
    }

    override def read(kryo: Kryo, input: Input, clz: Class[ContigLengths]): ContigLengths = {
      val size = input.readInt()
      SortedMap(
        (for {
          idx ← 0 until size
        } yield
          idx →
            (
              kryo.readClassAndObject(input).asInstanceOf[ContigName],
              NumLoci(input.readLong())
            )
        ): _*
      )
    }
  }
}
