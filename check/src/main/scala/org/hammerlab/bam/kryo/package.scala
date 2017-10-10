package org.hammerlab.bam

import java.util

import com.esotericsoftware.kryo.Serializer
import htsjdk.samtools._
import org.hammerlab.kryo.{ AlsoRegister, serializeAs }
import org.hammerlab.paths.Path

package object kryo {
  implicit val registerSAMFileHeader =
    AlsoRegister[SAMFileHeader](
      classOf[SAMFileHeader],
      classOf[util.LinkedHashMap[_, _]],
      classOf[util.ArrayList[_]],
      classOf[util.HashMap[_, _]],
      classOf[SAMReadGroupRecord],
      classOf[SAMSequenceDictionary],
      "scala.collection.convert.Wrappers$",
      classOf[SAMSequenceRecord],
      classOf[SAMProgramRecord],
      classOf[SAMFileHeader.GroupOrder],
      classOf[SAMFileHeader.SortOrder]
    )

  implicit val pathSerializer: Serializer[Path] = serializeAs[Path, String](_.toString, Path(_))
}
