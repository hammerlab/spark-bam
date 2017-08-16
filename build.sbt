name := "spark-bam"
version := "1.1.0-SNAPSHOT"

deps ++= Seq(
  bytes % "1.0.2",
  case_app,
  cats,
  channel % "1.0.0",
  hadoop_bam % "7.9.0",
  io % "1.1.0",
  iterators % "1.3.1-SNAPSHOT",
  magic_rdds % "2.2.0-SNAPSHOT",
  math % "1.0.0",
  paths % "1.2.0",
  reference % "1.4.0",
  slf4j % "1.3.1",
  spark_util % "1.3.0",
  stats % "1.0.1"
)

compileAndTestDeps += loci % "2.0.1"

addSparkDeps

shadedDeps += shapeless

// Spark 2.1.0 (spark-submit is an easy way to run this library's Main) puts shapeless 2.0.0 on the classpath, but we
// need 2.3.2.
shadeRenames ++= Seq(
  "shapeless.**" → "org.hammerlab.shapeless.@1"
)

main := "org.hammerlab.bam.spark.Main"

// It can be convenient to keep google-cloud-nio and gcs-connecter shaded JARs in lib/, though they're not checked into
// git. However, we exclude them from the assembly JAR by default, on the assumption that they'll be provided otherwise
// at runtime (by Dataproc in the case of gcs-connector, and by manually adding to the classpath in the case of
// google-cloud-nio).
assemblyExcludeLib
