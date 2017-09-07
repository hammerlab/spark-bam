package org.hammerlab.args

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }

import java.lang.{ Long ⇒ JLong }

import org.apache.spark.serializer.KryoSerializer
import org.hammerlab.bytes._
import org.hammerlab.spark.confs
import org.hammerlab.spark.test.serde.KryoSerialization.kryoBytes
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.serde.JavaSerialization._

class ByteRangesTest
  extends SparkSuite
    with confs.Kryo {

  override def registrationRequired: Boolean = false

  test("kryo serialization") {
    val ks = new KryoSerializer(sc.getConf)
    implicit val kryo = ks.newKryo()

    val bytes = kryoBytes(ByteRanges(Seq(Endpoints(10.MB, 20.MB))))
    bytes.length should be(69)
  }

  test("java serde") {
    val byteRanges = ByteRanges(Seq(Endpoints(10.MB, 20.MB)))
    javaRead[ByteRanges](javaBytes(byteRanges)) should be(byteRanges)
  }

}
