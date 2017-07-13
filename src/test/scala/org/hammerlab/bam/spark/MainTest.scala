package org.hammerlab.bam.spark

import java.lang.System.setProperty

import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File
import org.hammerlab.bytes._
import org.hammerlab.spark.test.suite.SparkConfBase

class MainTest
  extends Suite
    with SparkConfBase {

  setProperty("spark.driver.allowMultipleContexts", "true")

  // Register this class as its own KryoRegistrator
  setProperty("spark.kryo.registrator", classOf[Registrar].getCanonicalName)
  setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  setProperty("spark.kryo.referenceTracking", "false")
  setProperty("spark.kryo.registrationRequired", "true")

  setProperty("spark.hadoop.fs.gs.project.id", "pici-1286")
  setProperty("spark.hadoop.google.cloud.auth.service.account.enable", "true")
  setProperty("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/ryan/.credentials/PICI-ad317a11aff8.json")

  setSparkProps()

  def check(args: Args, expected: String): Unit = {
    val outPath = tmpPath()
    Main.run(
      args.copy(
        out = Some(outPath),
        threads = 8
      ),
      Seq[String](
        File("1.2203053-2211029.bam")
      )
    )

    outPath.read should be(expected.stripMargin)
  }

  test("eager 470KB") {
    check(
      Args(
        splitSize = Some(470.KB)
      ),
      """Split-size distribution:
        |num:	2,	mean:	474291.5,	stddev:	2723.5,	mad:	2723.5
        |elems:	471568, 477015
        |
        |2 splits:
        |	Split(0:45846,486847:7)
        |	Split(486847:7,963864:0)
        |"""
    )
  }

  test("seqdoop 470KB") {
    check(
      Args(
        seqdoop = true,
        splitSize = Some(470.KB)
      ),
      """Split-size distribution:
        |num:	2,	mean:	493351.5,	stddev:	5508.5,	mad:	5508.5
        |elems:	487843, 498860
        |
        |2 splits:
        |	Split(0:45846,481280:65535)
        |	Split(486847:6,963864:65535)
        |"""
    )
  }

  test("compare 470KB") {
    check(
      Args(
        compare = true,
        splitSize = Some(470.KB)
      ),
      """2 splits differ (totals: 2, 2):
        |		Split(486847:6,963864:65535)
        |	Split(486847:7,963864:0)
        |"""
    )
  }

  test("compare 480KB") {
    check(
      Args(
        compare = true,
        splitSize = Some(480.KB)
      ),
      """All splits matched!
        |
        |Split-size distribution:
        |num:	2,	mean:	474291.5,	stddev:	21385.5,	mad:	21385.5
        |elems:	495677, 452906
        |sorted:	452906, 495677
        |
        |2 splits:
        |	Split(0:45846,510891:202)
        |	Split(510891:202,963864:0)
        |"""
    )
  }
}
