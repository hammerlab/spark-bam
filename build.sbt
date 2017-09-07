
lazy val spark_bam = rootProject(app, bgzf, check, load, cli, seqdoop, test_bams)

lazy val app = project.settings(
  name := "app",
  version := "1.0.0-SNAPSHOT",
  deps ++= Seq(
    case_app,
    io % "1.2.0",
    paths % "1.2.0",
    slf4j % "1.3.1",
    spark_util % "1.3.0"
  ),
  addSparkDeps
)

lazy val bgzf = project.settings(
  name := "bgzf",
  version := "1.0.0-SNAPSHOT",
  deps ++= Seq(
    "org.hammerlab" ^^ "app" ^ "1.0.0-SNAPSHOT",
    case_app,
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
  testUtilsVersion := "1.3.2-SNAPSHOT",
  testDeps += "org.hammerlab.bam" ^^ "test-bams" ^ "1.0.0-SNAPSHOT"
)

lazy val check = project.settings(
  organization := "org.hammerlab.bam",
  name := "check",
  version := "1.0.0-SNAPSHOT",
  deps ++= Seq(
    "org.hammerlab" ^^ "bgzf" ^ "1.0.0-SNAPSHOT",
    ("org.seqdoop" ^ "hadoop-bam" ^ "7.8.0") - hadoop,
    case_app,
    cats,
    channel % "1.1.0-SNAPSHOT",
    htsjdk,
    magic_rdds % "3.0.0-SNAPSHOT",
    slf4j % "1.3.1",
    spark_util % "1.3.0"
  ),
  addSparkDeps,
  compileAndTestDeps += loci % "2.0.1",
  testDeps += "org.hammerlab.bam" ^^ "test-bams" ^ "1.0.0-SNAPSHOT"
)

lazy val seqdoop = project.settings(
  organization := "org.hammerlab.bam",
  name := "seqdoop",
  version := "1.0.0-SNAPSHOT"
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

lazy val load = project.settings(
  organization := "org.hammerlab.bam",
  name := "load",
  version := "1.0.0-SNAPSHOT",
  deps ++= Seq(
//    bytes % "1.0.2",
//    case_app,
//    cats,
//    channel % "1.0.0",
//    hadoop_bam % "7.9.0",
//    io % "1.2.0",
//    iterators % "1.4.0",
//    magic_rdds % "3.0.0-SNAPSHOT",
//    math % "2.0.0",
//    paths % "1.2.0",
//    reference % "1.4.0",
//    slf4j % "1.3.1",
//    spark_util % "1.3.0",
//    stats % "1.0.1"
  ),
//  compileAndTestDeps += loci % "2.0.1",
  addSparkDeps
)

lazy val cli = project.settings(
  organization := "org.hammerlab.bam",
  name := "cli",
  version := "1.0.0-SNAPSHOT",

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
)
