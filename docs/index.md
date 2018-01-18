Process [BAM files][SAM spec] using [Apache Spark] and [HTSJDK]; inspired by [hadoop-bam].

```bash
$ spark-shell --packages=org.hammerlab.bam:load_2.11:1.1.0
```
```scala
import spark_bam._, hammerlab.path._

val path = Path("test_bams/src/main/resources/2.bam")

// Load an RDD[SAMRecord] from `path`; supports .bam, .sam, and .cram
val reads = sc.loadReads(path)
// RDD[SAMRecord]

reads.count
// 2500

import hammerlab.bytes._

// Configure maximum split size
sc.loadReads(path, splitSize = 16 MB)
// RDD[SAMRecord]

// Only load reads in specific intervals
sc.loadBamIntervals(path)("1:13000-14000", "1:60000-61000").count
// 129
```

## Linking

### SBT

```scala
libraryDependencies += "org.hammerlab.bam" %% "load" % "1.1.0"
```

### Maven

```xml
<dependency>
  <groupId>org.hammerlab.bam</groupId>
  <artifactId>load_2.11</artifactId>
  <version>1.1.0</version>
</dependency>
```

### From `spark-shell`

```bash
spark-shell --packages=org.hammerlab.bam:load:1.1.0
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
spark-shell --jars $GOOGLE_CLOUD_NIO_JAR --packages=org.hammerlab.bam:load:1.1.0
…
import spark_bam._, hammerlab.path._

val reads = sc.loadBam(Path("gs://bucket/my.bam"))
```

<!-- External project links -->
[Apache Spark]: https://spark.apache.org/
[HTSJDK]: https://github.com/samtools/htsjdk
[google-cloud-nio]: https://github.com/GoogleCloudPlatform/google-cloud-java/tree/v0.10.0/google-cloud-contrib/google-cloud-nio
[SAM spec]: http://samtools.github.io/hts-specs/SAMv1.pdf

<!-- Repos -->
[hadoop-bam]: https://github.com/HadoopGenomics/Hadoop-BAM
[spark-bam]: https://github.com/hammerlab/spark-bam

[linking]: #linking
