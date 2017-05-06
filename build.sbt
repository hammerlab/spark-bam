name := "hadoop-bam"
version := "1.0.1-SNAPSHOT"
deps ++= Seq(
  libs.value('hadoop_bam),
  libs.value('iterators),
  libs.value('slf4j),
  "com.github.alexarchambault" %% "case-app" % "1.2.0-M3",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "1.6.0-hadoop2"
)

providedDeps += libs.value('hadoop)

// Spark 2.1.0 (spark-submit is an easy way to run this library's Main) puts shapeless 2.0.0 on the classpath, but we
// need 2.3.2.
shadeRenames ++= Seq(
  "shapeless.**" → "org.hammerlab.shapeless.@1",
  "com.google.common.base.**" → "org.hammerlab.guava.base.@1"
)

main := "org.hammerlab.hadoop_bam.Main"
