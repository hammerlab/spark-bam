import sbtassembly.PathList

name := "hadoop-bam"
version := "1.0.1-SNAPSHOT"
deps ++= Seq(
  libs.value('hadoop_bam),
  libs.value('iterators),
  libs.value('slf4j),
  "com.github.alexarchambault" %% "case-app" % "1.2.0-M3",
  "com.google.cloud" % "google-cloud-nio" % "0.10.0-alpha",
  libs.value('magic_rdds),
  libs.value('paths),
  libs.value('reference),
  "com.chuusai" %% "shapeless" % "2.3.2"
)

addSparkDeps

// Spark 2.1.0 (spark-submit is an easy way to run this library's Main) puts shapeless 2.0.0 on the classpath, but we
// need 2.3.2.
shadeRenames ++= Seq(
  "shapeless.**" → "org.hammerlab.shapeless.@1",

  // google-cloud-nio uses Guava 19.0 and at least one API (Splitter.splitToList) that succeeds Spark's Guava (14.0.1).
  "com.google.common.**" -> "org.hammerlab.guava.common.@1",

  // GCS Connector uses an older google-api-services-storage than google-cloud-nio and breaks us without shading it in
  // here; see http://stackoverflow.com/a/39521403/544236.
  "com.google.api.services.**" -> "hammerlab.google.api.services.@1"
)

main := "org.hammerlab.hadoop_bam.Main"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}
