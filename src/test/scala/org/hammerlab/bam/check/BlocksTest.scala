package org.hammerlab.bam.check

import org.apache.spark.{ SparkConf, SparkContext }
import org.hammerlab.bam.kryo.Registrar
import org.hammerlab.resources.tcgaBamExcerpt
import org.hammerlab.spark.test.suite.MainSuite
import org.hammerlab.test.Suite

class BlocksTest
  extends MainSuite(classOf[Registrar]) {
//  extends Suite {
  test("foo") {
    implicit val path = tcgaBamExcerpt.path

    val args: Args =
      Args(
        blocksPerPartition = 5
      )

//    val conf = new SparkConf(false)
//    conf.setMaster("local[4]")
//    conf.setAppName("BlocksTest")
//
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryo.referenceTracking", "false")
//    conf.set("spark.kryo.registrationRequired", "true")
//    conf.set("spark.kryo.registrator", classOf[Registrar].getCanonicalName)
//
//    implicit val sc = new SparkContext(conf)

//    val blocks = Blocks(args)(sc, path)
//
//    blocks.uncompressedSize should be(2580596)

//    val (calls, blocks) = eager.Run.getCalls(args)
//    val (calls, blocks) = eager.Run.getCalls(args)(sc, path)

    Main.run(
      Args(
        blocksPerPartition = 5,
        eager = true
      ),
      Seq[String](tcgaBamExcerpt)
    )

  }
}
