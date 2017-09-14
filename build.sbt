
lazy val spark_bam =
  rootProject(
    bgzf,
    check,
    cli,
    load,
    seqdoop,
    test_bams
  )

lazy val bgzf = project.settings(
  version := "1.0.0-SNAPSHOT",
  deps ++= Seq(
    case_app,
    case_cli ^ "1.0.0-SNAPSHOT",
    cats,
    channel % "1.1.0-SNAPSHOT",
    io % "1.2.0",
    iterators % "1.4.0",
    math % "2.0.0",
    paths % "1.2.1-SNAPSHOT",
    slf4j % "1.3.1",
    spark_util % "1.3.0",
    stats % "1.0.1"
  ),
  addSparkDeps,
  testUtilsVersion := "1.3.2-SNAPSHOT"
).dependsOn(
  test_bams % "test"
)

lazy val check = project.settings(
  organization := "org.hammerlab.bam",
  name := "check",
  version := "1.0.0-SNAPSHOT",
  deps ++= Seq(
    case_app,
    cats,
    channel % "1.1.0-SNAPSHOT",
    htsjdk,
    magic_rdds % "3.0.0-SNAPSHOT",
    seqdoop_hadoop_bam,
    slf4j % "1.3.1",
    spark_util % "1.3.0"
  ),
  fork := true,  // ByteRangesTest exposes an SBT bug that this works around; see https://github.com/sbt/sbt/issues/2824
  addSparkDeps,
  compileAndTestDeps += loci % "2.0.1",
  testUtilsVersion := "1.3.2-SNAPSHOT"
).dependsOn(
  bgzf,
  test_bams % "test"
)

lazy val cli = project.settings(
  organization := "org.hammerlab.bam",
  version := "1.0.0-SNAPSHOT",

  deps ++= Seq(
    hammerlab_hadoop_bam ^ "7.9.0",
    case_app,
    case_cli ^ "1.0.0-SNAPSHOT",
    cats,
    channel % "1.1.0-SNAPSHOT",
    iterators % "1.4.0",
    magic_rdds % "3.0.0-SNAPSHOT",
    paths % "1.2.1-SNAPSHOT",
    shapeless,
    spark_util % "1.3.0",
    stats % "1.0.1"
  ),

  // Bits that depend on the seqdoop module use org.hammerlab:hadoop-bam; make sure we don't get the org.seqdoop one.
  excludeDependencies += SbtExclusionRule("org.seqdoop", "hadoop-bam"),
  
  addSparkDeps,

  shadedDeps += shapeless,

  // Spark 2.1.0 (spark-submit is an easy way to run this library's Main) puts shapeless 2.0.0 on the classpath, but we
  // need 2.3.2.
  shadeRenames ++= Seq(
    "shapeless.**" → "org.hammerlab.shapeless.@1"
  ),

  main := "org.hammerlab.bam.Main",

  // It can be convenient to keep google-cloud-nio and gcs-connecter shaded JARs in lib/, though they're not checked into
  // git. However, we exclude them from the assembly JAR by default, on the assumption that they'll be provided otherwise
  // at runtime (by Dataproc in the case of gcs-connector, and by manually adding to the classpath in the case of
  // google-cloud-nio).
  assemblyExcludeLib
).dependsOn(
  bgzf,
  check,
  load,
  seqdoop,
  test_bams % "test"
)

lazy val load = project.settings(
  organization := "org.hammerlab.bam",
  version := "1.0.0-SNAPSHOT",
  deps ++= Seq(
    channel % "1.1.0-SNAPSHOT",
    htsjdk,
    iterators % "1.4.0",
    math % "2.0.0",
    paths % "1.2.1-SNAPSHOT",
    reference % "1.4.0",
    seqdoop_hadoop_bam,
    slf4j % "1.3.1",
    spark_util % "1.3.0"
  ),
  compileAndTestDeps += loci % "2.0.1",
  addSparkDeps,
  testDeps += magic_rdds % "3.0.0-SNAPSHOT"
).dependsOn(
  bgzf,
  check,
  test_bams % "test"
)

lazy val seqdoop = project.settings(
  organization := "org.hammerlab.bam",
  version := "1.0.0-SNAPSHOT",
  deps ++= Seq(
    channel % "1.1.0-SNAPSHOT",
    htsjdk,
    paths % "1.2.1-SNAPSHOT",
    hammerlab_hadoop_bam % "7.9.0"
  ),
  // Make sure we get org.hammerlab:hadoop-bam, not org.seqdoop
  excludeDependencies += SbtExclusionRule("org.seqdoop", "hadoop-bam"),
  addSparkDeps
).dependsOn(
  bgzf,
  check,
  test_bams % "test"
)

lazy val test_bams = project.settings(
  organization := "org.hammerlab.bam",
  name := "test-bams",
  version := "1.0.0-SNAPSHOT",
  deps ++= Seq(
    paths ^ "1.2.1-SNAPSHOT",
    testUtils ^ "1.3.2-SNAPSHOT"
  ),
  testDeps := Nil
)
