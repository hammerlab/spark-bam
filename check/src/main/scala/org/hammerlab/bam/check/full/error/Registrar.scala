package org.hammerlab.bam.check.full.error

import org.hammerlab.kryo._

import scala.collection.immutable.BitSet

class Registrar
  extends spark.Registrar(
    cls[BitSet],
    cls[Flags] → new FlagsSerializer
  )
