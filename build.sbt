name := "spark-bam"
version := "1.1.0-SNAPSHOT"
deps ++= Seq(
  case_app,
  hadoop_bam % "7.8.1-SNAPSHOT",
  iterators % "1.3.0-SNAPSHOT",
  magic_rdds % "1.5.0-SNAPSHOT",
  paths % "1.1.1-SNAPSHOT",
  reference % "1.3.1-SNAPSHOT",
  slf4j,
  spark_util % "1.2.0-SNAPSHOT"
)

compileAndTestDeps += loci % "2.0.0-SNAPSHOT"

addSparkDeps

testUtilsVersion := "1.2.4-SNAPSHOT"
sparkTestsVersion := "2.1.0-SNAPSHOT"

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
assemblyExcludedJars in assembly := {
  (fullClasspath in assembly).value.filter {
    _.data.getParent.endsWith("/lib")
  }
}
