name := "hadoop-bam"
version := "1.0.0-SNAPSHOT"
deps ++= Seq(
  libs.value('hadoop_bam),
  libs.value('slf4j),
  "com.github.alexarchambault" %% "case-app" % "1.2.0-M3"
)

providedDeps += libs.value('hadoop)

shadeRenames += ("shapeless.**" → "org.hammerlab.shapeless.@1")
