package org.hammerlab.bam.header

import java.io.InputStream

import hammerlab.path._
import htsjdk.samtools.SamReaderFactory.Option.EAGERLY_DECODE
import htsjdk.samtools.SamReaderFactory.makeDefault
import htsjdk.samtools.{ SAMFileHeader, SAMSequenceDictionary, SAMSequenceRecord, SamInputResource }
import org.hammerlab.bam.header.ContigLengths.ContigLengthsT
import org.hammerlab.genomics.reference
import org.hammerlab.genomics.reference.{ ContigName, NumLoci }
import org.hammerlab.hadoop.Configuration
import org.seqdoop.hadoop_bam.util.SAMHeaderReader._

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

  def readSAMHeaderFromStream(in: InputStream, conf: Configuration): SAMFileHeader = {
    val stringency = getValidationStringency(conf)

    val readerFactory =
      makeDefault
        .setOption(EAGERLY_DECODE, false)
        .setUseAsyncIo(false)

    if (stringency != null)
      readerFactory.validationStringency(stringency)

    val refSource = getReferenceSource(conf)

    if (null != refSource)
      readerFactory.referenceSource(refSource)

    readerFactory.open(SamInputResource.of(in)).getFileHeader
  }

  type ContigLengthsT = SortedMap[Int, (ContigName, NumLoci)]

  def apply(path: Path)(implicit conf: Configuration): ContigLengths =
    readSAMHeaderFromStream(path.inputStream, conf)

  def apply(sequences: Seq[(Int, (ContigName, NumLoci))]): ContigLengths =
    ContigLengths(
      SortedMap(
        sequences: _*
      )
    )

  implicit def wrapContigLengths(contigLengths: ContigLengthsT): ContigLengths = ContigLengths(contigLengths)
  implicit def unwrapContigLengths(contigLengths: ContigLengths): ContigLengthsT = contigLengths.map

  implicit def toReferenceContigLengths(contigLengths: ContigLengths): reference.ContigLengths =
    contigLengths
      .map
      .map {
        case (_, (contigName, numLoci)) ⇒
          contigName → numLoci
      }

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

  import org.hammerlab.kryo._

  implicit val serializer: Serializer[ContigLengths] =
    Serializer(
      (kryo, input) ⇒ {
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
      },
      (kryo, output, contigLengths) ⇒ {
        output.writeInt(contigLengths.size)
        for {
          (_, (name, length)) ← contigLengths
        } {
          kryo.writeClassAndObject(output, name)
          output.writeLong(length)
        }
      }
    )

  implicit val alsoRegister: AlsoRegister[ContigLengths] =
    AlsoRegister(
      cls[ContigName]
    )
}
