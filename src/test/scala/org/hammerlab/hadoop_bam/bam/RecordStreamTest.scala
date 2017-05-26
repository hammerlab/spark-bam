package org.hammerlab.hadoop_bam.bam

import java.io.{ IOException, PrintWriter }
import java.nio.channels.FileChannel
import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedDeque

import htsjdk.samtools.{ SAMException, SAMRecord }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.hammerlab.hadoop_bam.bgzf.Pos
import org.hammerlab.hadoop_bam.bgzf.block.{ Metadata, MetadataStream }
import org.hammerlab.iterator.Sliding2Iterator._
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File
import org.seqdoop.hadoop_bam.BAMSplitGuesser
import org.seqdoop.hadoop_bam.util.WrapSeekable

class RecordStreamTest
  extends Suite {

  sealed trait StreamOp

  case class Record(pos: Pos, start: Int, name: String) extends StreamOp
  implicit def makeRecord(t: (Pos, Int, String)): Record = Record(t._1, t._2, t._3)

  case class Drop(n: Int) extends StreamOp

  // Testing shorthand: convert int -> int to VirtualPos
  implicit class IntsToPos(val blockPos: Int) {
    def →(offset: Int) =
      Pos(blockPos, offset)
  }

  implicit def convTuple(t: (Pos, SAMRecord)): (Pos, Int, String) =
    (t._1, t._2.getAlignmentStart, t._2.getReadName)

  def check(ops: StreamOp*)(implicit rs: RecordStream): Unit =
    ops.foreach {
      case Drop(n) ⇒ rs.drop(n)
      case Record(pos, start, name) ⇒
        rs.curPos should be(Some(pos))
        check(pos, start, name)
    }

  def check(expectedPos: Pos, start: Int, name: String)(implicit rs: RecordStream): Unit = {
    val (pos, rec) = rs.next
    convTuple(pos, rec) should be(
      (expectedPos, start, name)
    )
  }

  test("5k") {
    val conf = new Configuration
    implicit val rs = SeekableRecordStream(File("test5k.bam"))

    check(
      ( 2454 →     0, 10001, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724"),
      ( 2454 →   624, 10009, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724"),
      ( 2454 →  1244, 10048, "HWI-ST807:461:C2P0JACXX:4:1304:9505:89866"),
      ( 2454 →  1883, 10335, "HWI-ST807:461:C2P0JACXX:4:2311:6431:65669"),
      ( 2454 →  2520, 10363, "HWI-ST807:461:C2P0JACXX:4:1305:2342:51860"),
      ( 2454 →  3088, 10363, "HWI-ST807:461:C2P0JACXX:4:1305:2342:51860"),
      ( 2454 →  3734, 10368, "HWI-ST807:461:C2P0JACXX:4:1304:9505:89866"),
      ( 2454 →  4368, 10458, "HWI-ST807:461:C2P0JACXX:4:2311:6431:65669"),
      ( 2454 →  4987, 11648, "HWI-ST807:461:C2P0JACXX:4:1107:13461:64844"),
      ( 2454 →  5668, 11687, "HWI-ST807:461:C2P0JACXX:4:2203:17157:59976"),
      Drop(20),
      ( 2454 → 18684, 11911, "HWI-ST807:461:C2P0JACXX:4:2302:17669:14421"),
      Drop(20),
      ( 2454 → 32162, 12123, "HWI-ST807:461:C2P0JACXX:4:2101:17257:40389"),
      Drop(20),
      ( 2454 → 45238, 12298, "HWI-ST807:461:C2P0JACXX:4:1312:6062:40537"),
      Drop(20),
      ( 2454 → 58258, 12572, "HWI-ST807:461:C2P0JACXX:4:1205:8857:43215"),
      Drop(9),
      ( 2454 → 64473, 12656, "HWI-ST807:461:C2P0JACXX:4:2103:9923:17201"),
      (27784 →     0, 12676, "HWI-ST807:461:C2P0JACXX:4:2310:1258:47908"),
      (27784 →   664, 12682, "HWI-ST807:461:C2P0JACXX:4:2302:19124:35915")
    )

    rs.seek(Pos(2454, 64473))
    rs.curPos should be(Some(Pos(2454, 64473)))

    check(
      ( 2454 → 64473, 12656, "HWI-ST807:461:C2P0JACXX:4:2103:9923:17201"),
      (27784 →     0, 12676, "HWI-ST807:461:C2P0JACXX:4:2310:1258:47908"),
      (27784 →   664, 12682, "HWI-ST807:461:C2P0JACXX:4:2302:19124:35915")
    )

    rs.seek(Pos(0, 0))

    check(
      ( 2454 →     0, 10001, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724"),
      ( 2454 →   624, 10009, "HWI-ST807:461:C2P0JACXX:4:2115:8592:79724")
    )

    rs.curPos should be(Some(Pos(2454, 1244)))
    rs.seek(Pos(2454, 1244))
    rs.curPos should be(Some(Pos(2454, 1244)))
    check(
      ( 2454 →  1244, 10048, "HWI-ST807:461:C2P0JACXX:4:1304:9505:89866")
    )
  }

  test("metadata") {
    val ch = FileChannel.open(Paths.get(File("1.bam")))
    MetadataStream(ch).size should be(0)
    MetadataStream(ch).take(10).toList should be(
      List(
        Metadata(     0, 65498, 15071),
        Metadata( 15071, 65498, 25969),
        Metadata( 41040, 65498, 26000),
        Metadata( 67040, 65498, 26842),
        Metadata( 93882, 65498, 25118),
        Metadata(119000, 65498, 25017),
        Metadata(144017, 65498, 25814),
        Metadata(169831, 65498, 24814),
        Metadata(194645, 65498, 24059),
        Metadata(218704, 65498, 24597)
      )
    )
  }

  test("2.bam") {
    val conf = new Configuration
    val pathStr = "2.bam"
    val path = new Path(File(pathStr).uri)
    val fs = path.getFileSystem(conf)
    val ss = WrapSeekable.openPath(conf, path)

    val stream = MetadataStream(FileChannel.open(File(pathStr).path))

    var continue = true

    val timerThread =
      new Thread {
        override def run(): Unit = {
          while (continue) {
            Thread.sleep(1000)
            //println(s"recs: $recs, blocks: ${stream.blockIdx}")
            println(s"blocks: ${stream.blockIdx}")
          }
          println("timer thread exiting")
        }
      }

    timerThread.start()

    val bp = new PrintWriter(fs.create(new Path("/Users/ryan/c/hl/hadoop-bam/blocks2")))

    try {
      while (stream.hasNext) {
        val Metadata(start, uncompressedSize, _) = stream.next()
        bp.println(s"$start,$uncompressedSize")
      }
    } catch {
      case e: SAMException ⇒
        println(
          s"caught exception at ${stream.ch.position()}"
        )
      case e: IOException ⇒
        println(
          s"caught exception at ${stream.ch.position()}"
        )
    }

    continue = false
    println(s"last block idx: ${stream.blockIdx}")
    bp.close()

    stream.blockIdx should be(13380)
  }

  case class Worker(id: Int,
                    queue: ConcurrentLinkedDeque[Long],
                    joinedRecordPosMap: Map[Long, (Option[Pos], Vector[Int])],
                    blockSizeMap: Map[Long, Int])
    extends Thread {

    var blockPos: Long = -1
    var up: Int = -1
    var usize: Int = -1


    override def toString: String =
      s"$id:${Pos(blockPos, up)}/$usize"

    override def run(): Unit = {
      val conf = new Configuration
      val pathStr = "2.bam"
      val path = new Path(File(pathStr).uri)
      val fs = path.getFileSystem(conf)
      val ss = WrapSeekable.openPath(conf, path)
      val guesser = new BAMSplitGuesser(ss, conf)

      while (true) {
        Option(queue.poll()) match {
          case None ⇒
            return
          case Some(bp) ⇒
            blockPos = bp
            println(s"Processing $blockPos")
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
              var guess = guesser.findNextBAMPos(0, up)
              if (guess == -1) {
                throw new Exception(s"No guess at ${Pos(blockPos, up)} ($usize): expected ${it.head}")
              } else if (Pos(guess + (blockPos << 16)) != it.head) {
                throw new Exception(
                  s"Bad guess at ${Pos(blockPos, up)} ($usize): expected ${it.head}, actual ${Pos(guess + (blockPos << 16))}"
                )
              }

              up += 1
            }
        }
      }
    }
  }

  test("all blocks") {
    val blockSizeMap =
      File("blocks")
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
      File("records")
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

    val numWorkers = 1

    val workers =
      for { id ← 0 until numWorkers } yield
        Worker(
          id,
          queue,
          joinedRecordPosMap,
          blockSizeMap
        )

    val timerThread =
      new Thread {
        override def run(): Unit = {
          while (!queue.isEmpty) {
            println(s"Blocks remaining: ${queue.size()}")
            println(workers.mkString("\t", "\n\t", "\n"))
            Thread.sleep(1000)
          }
        }
      }

    timerThread.start()

    workers.foreach(_.start)
    workers.foreach(_.join)

  }

  test("1.bam") {
    val conf = new Configuration
    val pathStr = "1.bam"
    val path = new Path(File(pathStr).uri)
    val fs = path.getFileSystem(conf)
    val ss = WrapSeekable.openPath(conf, path)
    val guesser = new BAMSplitGuesser(ss, conf)

    implicit val rs = RecordStream(File(pathStr))

    rs.drop(1000)

    var Pos(blockPos, offset) = rs.curPos.get
    guesser.fillBuffer(blockPos)

    for {
      ((pos, _), idx) ← rs.take(200).zipWithIndex
      block = rs.curBlock.get
      uncompressedSize = block.uncompressedSize
    } {
      println(s"$idx: looping from ${Pos(blockPos, offset)} to $pos")
      while (Pos(blockPos, offset) <= pos) {
//        println(s"testing ${Pos(blockPos, offset)}")
        val block = rs.curBlock.get
        var guess = guesser.findNextBAMPos(0, offset)
//        if (pos.blockPos > blockPos)
//          guess should be (-1)
//        else if (pos.blockPos == blockPos) {
        withClue(s"$idx: expected $pos from ${Pos(blockPos, offset)} ($uncompressedSize): ") {
          guess should not be (-1)
          Pos(guess + (blockPos << 16)) should be(pos)
        }
//        } else
//          fail(s"Current pos ${Pos(blockPos, offset)} > $pos")

        offset += 1
        if (offset == uncompressedSize) {
          offset = 0
          println(s"$idx: moving from block $blockPos to ${pos.blockPos}")
          blockPos = pos.blockPos
          guesser.fillBuffer(blockPos)
        }
      }
    }


//    {
//      val guesser = new BAMSplitGuesser(ss, conf)
//      val block = rs.curBlock.get
//      guesser.fillBuffer(blockPos)
//      var guess = guesser.findNextBAMPos(0, offset, block.uncompressedSize)
//      guess should not be (-1)
//      Pos(guess) should be(Pos(blockPos, offset))
//    }

//    Pos(guesser.guessNextBAMRecordStart(0, 335544320L)) should be(Pos(0, 45846))
  }

}
