package org.hammerlab.bam.check.indexed

import hammerlab.iterator._
import org.hammerlab.bam.check
import org.hammerlab.bam.check.Checker.MakeChecker
import org.hammerlab.bam.check.{ MaxReadSize, ReadStartFinder }
import org.hammerlab.bgzf.Pos
import org.hammerlab.channel.{ CachingChannel, SeekableByteChannel }

import scala.collection.immutable.SortedSet

case class Checker(readPositions: SortedSet[Pos])
  extends check.Checker[Boolean]
    with ReadStartFinder {

  override def apply(pos: Pos): Boolean =
    readPositions(pos)

  override def nextReadStart(start: Pos)(
      implicit
      maxReadSize: MaxReadSize
  ): Option[Pos] =
    readPositions
      .iteratorFrom(start)
      .buffered
      .headOption
}

object Checker {
  implicit def makeChecker(implicit records: SortedSet[Pos]): MakeChecker[Boolean, Checker] =
    new MakeChecker[Boolean, Checker] {
      override def apply(ch: CachingChannel[SeekableByteChannel]): Checker =
        Checker(records)
    }
}
