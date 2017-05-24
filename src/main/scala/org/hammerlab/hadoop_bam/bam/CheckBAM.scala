package org.hammerlab.hadoop_bam.bam

import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedDeque

import caseapp._
import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path ⇒ HPath }
import org.hammerlab.hadoop_bam.bgzf.Pos
import org.hammerlab.iterator.Sliding2Iterator._
import org.hammerlab.paths.Path
import org.seqdoop.hadoop_bam.BAMSplitGuesser
import org.seqdoop.hadoop_bam.util.WrapSeekable

case class Args(@ExtraName("n") numWorkers: Int = 4,
                @ExtraName("k") blocksFile: String,
                @ExtraName("r") recordsFile: String,
                @ExtraName("b") bamFile: String,
                @ExtraName("i") printInterval: Int = 10)

case class Worker(id: Int,
                  pathStr: String,
                  queue: ConcurrentLinkedDeque[Long],
                  joinedRecordPosMap: Map[Long, (Option[Pos], Vector[Int])],
                  blockSizeMap: Map[Long, Int])
  extends Thread
    with Logging {

  var blockPos: Long = -1
  var up: Int = -1
  var usize: Int = -1

  override def toString: String =
    s"$id:${Pos(blockPos, up)}/$usize"

  override def run(): Unit = {
    val conf = new Configuration
    val path = new HPath(pathStr)
    val fs = path.getFileSystem(conf)
    val ss = WrapSeekable.openPath(conf, path)
    val guesser = new BAMSplitGuesser(ss, conf)

    while (true) {
      Option(queue.poll()) match {
        case None ⇒
          return
        case Some(bp) ⇒
          blockPos = bp
          logger.info(s"$id: processing $blockPos")
          val (nextBlockPosOpt, offsets) = joinedRecordPosMap(blockPos)
          usize = blockSizeMap(blockPos)
          guesser.fillBuffer(blockPos)
          val it =
            (
              offsets.iterator.map(offset ⇒ Pos(blockPos, offset)) ++
                nextBlockPosOpt.iterator
            )
            .buffered

          up = 0
          while (up < usize) {
            if (Pos(blockPos, up) > it.head) {
              it.next
            }
            val guess = guesser.findNextBAMPos(0, up)
            if (guess == -1) {
              logger.error(s"No guess at ${Pos(blockPos, up)} ($usize): expected ${it.head}")
            } else if (Pos(guess + (blockPos << 16)) != it.head) {
              logger.error(
                s"Bad guess at ${Pos(blockPos, up)} ($usize): expected ${it.head}, actual ${Pos(guess + (blockPos << 16))}"
              )
            }

            up += 1
          }
      }
    }
  }
}

object CheckBAM
  extends CaseApp[Args]
    with Logging {

  override def run(args: Args, remainingArgs: RemainingArgs): Unit = {
    val blockSizeMap =
      Path(new URI(args.blocksFile))
        .lines
        .map(
          _.split(",") match {
            case Array(pos, usize) ⇒
              pos.toLong → usize.toInt
            case a ⇒
              throw new IllegalArgumentException(s"Bad block line: ${a.mkString(",")}")
          }
        )
        .toMap

    val queue = new ConcurrentLinkedDeque[Long]()
    for { (block, _) ← blockSizeMap } { queue.add(block) }

    val recordPosMap =
      Path(new URI(args.recordsFile))
        .lines
        .map(
          _.split(",") match {
            case Array(a, b) ⇒
              Pos(a.toInt, b.toInt)
            case a ⇒
              throw new IllegalArgumentException(s"Bad record-pos line: ${a.mkString(",")}")
          }
        )
        .toVector
        .groupBy(_.blockPos)
        .mapValues(
          _
          .map(_.offset)
          .sorted
        )

    val joinedRecordPosMap =
      recordPosMap
        .toVector
        .sortBy(_._1)
        .sliding2Opt
        .map {
          case ((pos, offsets), nextPosOpt) ⇒
            pos →
              (
                nextPosOpt.map {
                  case (nextBlock, nextOffsets) ⇒
                    Pos(nextBlock, nextOffsets.head)
                } →
                  offsets
                )
        }
        .toMap

    val workers =
      for { id ← 0 until args.numWorkers } yield
        Worker(
          id,
          args.bamFile,
          queue,
          joinedRecordPosMap,
          blockSizeMap
        )

    val timerThread =
      new Thread {
        override def run(): Unit = {
          while (!queue.isEmpty) {
            logger.info(s"Blocks remaining: ${queue.size()}:\n${workers.mkString("\t", "\n\t", "\n")}")
            Thread.sleep(args.printInterval * 1000)
          }
        }
      }

    timerThread.start()

    workers.foreach(_.start)
    workers.foreach(_.join)
  }
}
