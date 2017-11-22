package org.hammerlab.bam.check

import hammerlab.iterator._
import hammerlab.path._
import magic_rdds.zip._
import org.apache.spark.rdd.RDD
import org.hammerlab.bam.check
import org.hammerlab.bam.check.Checker.MakeChecker
import org.hammerlab.bam.check.full.error.Flags
import org.hammerlab.bam.check.indexed.BlocksAndIndexedRecords
import org.hammerlab.bam.kryo.pathSerializer
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ Metadata, PosIterator }
import org.hammerlab.channel.CachingChannel._
import org.hammerlab.channel.SeekableByteChannel
import org.hammerlab.kryo._

import scala.collection.mutable

trait CallPartition {
  self: CheckerApp[_] ⇒

  def callPartition[
      C1 <: check.Checker[Boolean],
      Call2,
      C2 <: check.Checker[Call2]
  ](
      blocks: Iterator[Metadata]
  )(
      implicit
      makeChecker1: MakeChecker[Boolean, C1],
      makeChecker2: MakeChecker[Call2, C2]
  ): Iterator[(Pos, (Boolean, Call2))] = {

    val ch = SeekableByteChannel(path).cache
    val checker1 = makeChecker1(ch)
    val checker2 = makeChecker2(ch)

    blocks
      .flatMap {
        block ⇒
          compressedSizeAccumulator.add(block.compressedSize)
          PosIterator(block)
      }
      .map {
        pos ⇒
          pos →
            (
              checker1(pos),
              checker2(pos)
            )
      }
      .finish(ch.close())
  }

  def vsIndexed[Call, C <: Checker[Call]](
      implicit makeChecker: MakeChecker[Call, C]
  ): RDD[(Pos, (Boolean, Call))] = {
    val BlocksAndIndexedRecords(blocks, records) = BlocksAndIndexedRecords()
    blocks
      .zippartitions(records) {
        (blocks, setsIter) ⇒
          implicit val records = setsIter.next()
          callPartition[indexed.Checker, Call, C](blocks)
      }
  }
}

object CallPartition
  extends spark.Registrar(
    cls[Path],
    cls[mutable.WrappedArray.ofRef[_]],
    cls[mutable.WrappedArray.ofInt],
    cls[Flags]
  )
