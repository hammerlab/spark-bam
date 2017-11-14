Process [BAM files][SAM spec] using [Apache Spark] and [HTSJDK]; inspired by [hadoop-bam].

http://hammerlab.org/spark-bam/

```bash
$ spark-shell --packages=org.hammerlab.bam:load:1.0.0-SNAPSHOT
```
```scala
import spark_bam._, hammerlab.path._

val path = Path("test_bams/src/main/resources/2.bam")

// Load an RDD[SAMRecord] from `path`; supports .bam, .sam, and .cram
val reads = sc.loadReads(path)
// RDD[SAMRecord]

reads.count
// 2500

import org.hammerlab.bytes._

// Configure maximum split size
sc.loadReads(path, splitSize = 16 MB)
// RDD[SAMRecord]
```

## Linking

### SBT

```scala
libraryDependencies += "org.hammerlab.bam" %% "load" % "1.0.0-SNAPSHOT"
```

### Maven

```xml
<dependency>
       <groupId>org.hammerlab.bam</groupId>
       <artifactId>load_2.11</artifactId>
       <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### From `spark-shell`

```bash
spark-shell --packages=org.hammerlab.bam:load:1.0.0-SNAPSHOT
```

### On Google Cloud

[spark-bam] uses Java NIO APIs to read files, and needs the [google-cloud-nio] connector in order to read from Google Cloud Storage (`gs://` URLs).

Download a shaded [google-cloud-nio] JAR:

```bash
GOOGLE_CLOUD_NIO_JAR=google-cloud-nio-0.20.0-alpha-shaded.jar
wget https://oss.sonatype.org/content/repositories/releases/com/google/cloud/google-cloud-nio/0.20.0-alpha/$GOOGLE_CLOUD_NIO_JAR
```

Then include it in your `--jars` list when running `spark-shell` or `spark-submit`:

```bash
spark-shell --jars $GOOGLE_CLOUD_NIO_JAR --packages=org.hammerlab.bam:load:1.0.0-SNAPSHOT
…
import spark_bam._, hammerlab.path._

val reads = sc.loadBam(Path("gs://bucket/my.bam"))
```

<!-- Intra-page links -->
[checks table]: #improved-record-boundary-detection-robustness
[getting an assembly JAR]: #get-an-assembly-JAR
[required path arg]: #required-argument-path
[api-clarity]: #algorithm-api-clarity

<!-- Checker links -->
[`eager`]: #eager
[`seqdoop`]: #seqdoop
[`full`]: #full
[`indexed`]: #indexed

<!-- Checkers -->
[eager/Checker]: https://github.com/hammerlab/spark-bam/blob/master/check/src/main/scala/org/hammerlab/bam/check/eager/Checker.scala
[full/Checker]: https://github.com/hammerlab/spark-bam/blob/master/check/src/main/scala/org/hammerlab/bam/check/full/Checker.scala
[seqdoop/Checker]: https://github.com/hammerlab/spark-bam/blob/master/seqdoop/src/main/scala/org/hammerlab/bam/check/seqdoop/Checker.scala
[indexed/Checker]: https://github.com/hammerlab/spark-bam/blob/master/check/src/main/scala/org/hammerlab/bam/check/indexed/Checker.scala

[`Checker`]: src/main/scala/org/hammerlab/bam/check/Checker.scala

<!-- test/resources links -->
[`cli/src/test/resources/test-bams`]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/resources/test-bams
[output/check-bam]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/resources/output/check-bam
[output/full-check]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/resources/output/full-check

<!-- External project links -->
[Apache Spark]: https://spark.apache.org/
[HTSJDK]: https://github.com/samtools/htsjdk
[Google Cloud Dataproc]: https://cloud.google.com/dataproc/
[bigdata-interop]: https://github.com/GoogleCloudPlatform/bigdata-interop/
[google-cloud-nio]: https://github.com/GoogleCloudPlatform/google-cloud-java/tree/v0.10.0/google-cloud-contrib/google-cloud-nio
[SAM spec]: http://samtools.github.io/hts-specs/SAMv1.pdf

<!-- Repos -->
[hadoop-bam]: https://github.com/HadoopGenomics/Hadoop-BAM
[spark-bam]: https://github.com/hammerlab/spark-bam
[hammerlab/hadoop-bam]: https://github.com/hammerlab/Hadoop-BAM/tree/7.9.0

[`BAMSplitGuesser`]: https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java

<!-- Command/Subcommand links -->
[Main]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/Main.scala

[`check-bam`]: #check-bam
[check/Main]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/check/Main.scala

[`full-check`]: #full-check
[full/Main]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/check/full/Main.scala

[`compute-splits`]: #compute-splits
[spark/Main]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/spark/Main.scala

[`compare-splits`]: #compare-splits
[compare/Main]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/compare/Main.scala

[`index-blocks`]: #index-blocks
[IndexBlocks]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bgzf/index/IndexBlocks.scala
[`IndexBlocksTest`]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/scala/org/hammerlab/bgzf/index/IndexBlocksTest.scala

[`index-records`]: #index-records
[IndexRecords]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/index/IndexRecords.scala
[`IndexRecordsTest`]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/scala/org/hammerlab/bam/index/IndexRecordsTest.scala

[`htsjdk-rewrite`]: #htsjdk-rewrite
[rewrite/Main]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/rewrite/Main.scala

[`org.hammerlab.paths.Path`]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala
[Path NIO ctor]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala#L14
[Path URI ctor]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala#L157
[Path String ctor]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala#L145-L155

[`SAMRecord`]: https://github.com/samtools/htsjdk/blob/2.9.1/src/main/java/htsjdk/samtools/SAMRecord.java

[`LociSet`]: https://github.com/hammerlab/genomic-loci/blob/2.0.1/src/main/scala/org/hammerlab/genomics/loci/set/LociSet.scala

[`Pos`]: https://github.com/hammerlab/spark-bam/blob/master/bgzf/src/main/scala/org/hammerlab/bgzf/Pos.scala
[`Split`]: https://github.com/hammerlab/spark-bam/blob/master/check/src/main/scala/org/hammerlab/bam/spark/Split.scala

[linking]: #linking

[test_bams]: test_bams/src/main/resources
[cli/str/slice]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/resources/slice

[cli]: https://github.com/hammerlab/spark-bam/blob/master/cli
