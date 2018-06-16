
import genomics.{ loci, reference }

val `cli-base`  = hammerlab.cli. base +testtest
val `cli-spark` = hammerlab.cli.spark +testtest

default(
  // most modules in this project are published with this group
  subgroup("bam"),
  github.repo("spark-bam"),
  v"1.2.0-M1",
  versions(
                   bytes → "1.2.0"          ,
               `cli-base`→ "1.0.0"          ,
              `cli-spark`→ "1.0.0"          ,
                 channel → "1.5.1"          ,
    hammerlab.hadoop_bam → "7.9.0"          ,
            hammerlab.io → "5.1.1"          ,
               iterators → "2.2.0"          ,
                    loci → "2.2.0"          ,
              magic_rdds → "4.2.3"          ,
              math.utils → "2.2.0"          ,
                   paths → "1.5.0"          ,
               reference → "1.5.0"          ,
              spark_util → "3.0.0"          ,
                   stats → "1.3.1"          ,
                   types → "1.2.0"          ,
      seqdoop_hadoop_bam → "7.9.2"
  )
)

lazy val bgzf = project.settings(
  group("org.hammerlab"),
  dep(
   `cli-base`,
    cats,
    channel,
    hammerlab.io,
    iterators,
    math.utils,
    paths,
    slf4j,
    spark_util,
    stats
  ),
  spark
).dependsOn(
  `test-bams` test
)

lazy val check = project.settings(
  dep(
    bytes,
   `cli-base`,
    case_app,
    cats,
    channel,
    htsjdk,
    iterators,
    loci + testtest,
    magic_rdds,
    hammerlab.io,
    paths,
    seqdoop_hadoop_bam,
    slf4j,
    spark_util
  ),
  spark,
  fork := true  // ByteRangesTest exposes an SBT bug that this works around; see https://github.com/sbt/sbt/issues/2824
).dependsOn(
  bgzf,
  `test-bams` test
)

lazy val cli = project.settings(
  dep(
    bytes,
    case_app,
    cats,
    channel,
    hammerlab.hadoop_bam,
   `cli-base`,
   `cli-spark`,
    hammerlab.io,
    iterators,
    magic_rdds,
    paths,
    spark_util,
    stats,
    types
  ),

  // Bits that depend on the seqdoop module use org.hammerlab:hadoop-bam; make sure we don't get the org.seqdoop one.
  excludes += seqdoop_hadoop_bam,

  spark,

  shadedDeps += shapeless,

  // Spark 2.1.0 (spark-submit is an easy way to run this library's Main) puts shapeless 2.0.0 on the classpath, but we
  // need 2.3.2.
  shadeRenames += "shapeless.**" → "shaded.shapeless.@1",

  main := "org.hammerlab.bam.Main",

  // It can be convenient to keep google-cloud-nio and gcs-connecter shaded JARs in lib/, though they're not checked into
  // git. However, we exclude them from the assembly JAR by default, on the assumption that they'll be provided otherwise
  // at runtime (by Dataproc in the case of gcs-connector, and by manually adding to the classpath in the case of
  // google-cloud-nio).
  assemblyExcludeLib,

  publishAssemblyJar,

  consolePkg("spark_bam"),

  buildInfoPackage := "build_info.spark_bam",
  buildInfoObject := "cli"
).dependsOn(
  bgzf,
  check,
  load,
  seqdoop,
  `test-bams` test
).enablePlugins(
  BuildInfoPlugin
)

lazy val load = project.settings(

  // When running all tests in this project with `sbt test`, sometimes a Kryo
  // "Class is not registered: org.hammerlab.genomics.loci.set.LociSet" exception is thrown by
  // LoadBAMTest:"indexed disjoint regions"; this works around it.
  fork := true,

  dep(
    channel,
    htsjdk,
    iterators,
    loci + testtest,
    magic_rdds % tests,
    math.utils,
    paths,
    reference,
    seqdoop_hadoop_bam,
    shapeless,
    slf4j,
    spark_util
  ),
  spark
).dependsOn(
  bgzf,
  check,
  `test-bams` test
)

lazy val seqdoop = project.settings(
  dep(
    channel,
    hammerlab.hadoop_bam,
    htsjdk,
    paths
  ),
  // Make sure we get org.hammerlab:hadoop-bam, not org.seqdoop
  excludes += seqdoop_hadoop_bam,
  spark
).dependsOn(
  bgzf,
  check,
  `test-bams` test
)

lazy val `test-bams` = project.settings(
  name := "test-bams",
  v"1.1.0-M1",
  `2.11`.only,
  dep(
    paths,
    hammerlab.test.base
  ),
  clearTestDeps,
  test in sbt.Test := {}
)

// named this module "metrics" instead of "benchmarks" to work around bizarre IntelliJ-scala-plugin bug, cf.
// https://youtrack.jetbrains.com/issue/SCL-12628#comment=27-2439322
lazy val metrics = project.in(file("benchmarks")).settings(
  dep(
    paths,
    bytes
  )
)

lazy val `spark-bam` =
  root(
    bgzf,
    check,
    cli,
    load,
    seqdoop,
    `test-bams`
  )
