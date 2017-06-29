import sbtassembly.PathList

name := "spark-bam"
version := "1.1.0-SNAPSHOT"
deps ++= Seq(
  libs.value('iterators).copy(revision = "1.3.0-SNAPSHOT"),
  libs.value('slf4j),
  "com.github.alexarchambault" %% "case-app" % "1.2.0-M3",
  "org.hammerlab" %% "magic-rdds" % "1.5.0-SNAPSHOT",
  libs.value('paths),
  libs.value('reference),
  "org.hammerlab" %% "spark-util" % "1.1.3",
  "org.hammerlab" % "hadoop-bam" % "7.8.1-SNAPSHOT" exclude("org.apache.hadoop", "hadoop-client")
)

compileAndTestDeps += libs.value('loci)

addSparkDeps

shadedDeps += "com.chuusai" %% "shapeless" % "2.3.2"

// Spark 2.1.0 (spark-submit is an easy way to run this library's Main) puts shapeless 2.0.0 on the classpath, but we
// need 2.3.2.
shadeRenames ++= Seq(
  "shapeless.**" → "org.hammerlab.shapeless.@1"
)

main := "org.hammerlab.hadoop_bam.Main"
