name := "hadoop-bam"
version := "1.0.0"
deps ++= Seq(
  libs.value('hadoop_bam),
  libs.value('slf4j),
  "com.github.alexarchambault" %% "case-app" % "1.2.0-M3"
)

providedDeps += libs.value('hadoop)

// Spark 2.1.0 (spark-submit is an easy way to run this library's Main) puts shapeless 2.0.0 on the classpath, but we
// need 2.3.2.
shadeRenames += ("shapeless.**" → "org.hammerlab.shapeless.@1")

main := "org.hammerlab.hadoop_bam.Main"
