# spark-bam
[![Build Status](https://travis-ci.org/hammerlab/spark-bam.svg?branch=master)](https://travis-ci.org/hammerlab/spark-bam)

Process [BAM files][SAM spec] using [Apache Spark] and [HTSJDK]; inspired by [hadoop-bam].

```bash
$ spark-shell --packages=org.hammerlab.bam:load:1.0.0-SNAPSHOT
```
```scala
import org.hammerlab.bam.spark._
import org.hammerlab.paths.Path

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

## Using [spark-bam]

With [spark-bam on the classpath][linking], a `SparkContext` can be "enriched" with relevant methods for loading BAM files by importing:
   
```scala
import org.hammerlab.bam.spark._
```

### [`loadReads`]

The primary method exposed is `loadReads`, which will load an `RDD` of [HTSJDK `SAMRecord`s][`SAMRecord`] from a `.sam`, `.bam`, or `.cram` file:

```scala
sc.loadReads(path)
// RDD[SAMRecord]
```

Arguments:

- `path` (required)
	- an [`org.hammerlab.paths.Path`]
	- can be constructed [from a `URI`][Path URI ctor], [`String` (representing a `URI`)][Path String ctor], [or `java.nio.file.Path`][Path NIO ctor]:

		```scala
		import org.hammerlab.paths.Path
		val path = Path("test_bams/src/main/resources/2.bam")
		```
- `bgzfBlocksToCheck`: optional; default: 5
- `readsToCheck`: 
	- optional; default: 10
	- number of consecutive reads to verify when determining a record/split boundary 
- `maxReadSize`: 
	- optional; default: 10000000
	- throw an exception if a record boundary is not found in this many (uncompressed) positions
- `splitSize`: 
	- optional; default: taken from underlying Hadoop filesystem APIs
	- 

### [`loadBamIntervals`]

When the `path` is known to be an indexed `.bam` file, reads can be loaded that from only specified genomic-loci regions:

```scala
import org.hammerlab.genomics.loci.parsing.ParsedLoci
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.bam.header.ContigLengths
import org.hammerlab.hadoop.Configuration

implicit val conf: Configuration = sc.hadoopConfiguration

val parsedLoci = ParsedLoci("1:11000-12000,1:60000-")
val contigLengths = ContigLengths(path)

// "Join" `parsedLoci` with `contigLengths to e.g. resolve open-ended intervals
val loci = LociSet(parsedLoci, contigLengths)

sc.loadBamIntervals(
	path, 
	loci
)
// RDD[SAMRecord] with only reads overlapping [11000-12000) and [60000,∞) on chromosome 1
```

Arguments:

- `path` (required)
- `loci` (required): [`LociSet`] indicating genomic intervals to load
- `splitSize`: optional; default: taken from underlying Hadoop filesystem APIs
- `estimatedCompressionRatio`
	- optional; default: `3.0`
	- minor parameter used for approximately balancing Spark partitions; shouldn't be necessary to change

### [`loadReadsAndPositions`]

Implementation of [`loadReads`]: takes the same arguments, but returns [`SAMRecord`]s keyed by BGZF position ([`Pos`]).

Primarly useful for analyzing split-computations, e.g. in the [`compute-splits`] command.

### [`loadSplitsAndReads`]

Similar to [`loadReads`], but also returns computed [`Split`]s alongside the `RDD[SAMRecord]`.

Primarly useful for analyzing split-computations, e.g. in the [`compute-splits`] command.

## Linking against [spark-bam]

### As a library

#### Depend on [spark-bam] using Maven

```xml
<dependency>
	<groupId>org.hammerlab.bam</groupId>
	<artifactId>load_2.11</artifactId>
	<version>1.0.0-SNAPSHOT</version>
</dependency>
```

#### Depend on [spark-bam] using SBT

```scala
libraryDependencies += "org.hammerlab.bam" %% "load" % "1.0.0-SNAPSHOT"
```

### From `spark-shell`

#### After [getting an assembly JAR]

```bash
spark-shell --jars $SPARK_BAM_JAR
```

#### Using Spark packages

```bash
spark-shell --packages=org.hammerlab.bam:load:1.0.0-SNAPSHOT
```

```scala
spark-shell --jars $SPARK_BAM_JAR
…
import org.hammerlab.bam.spark._
import org.hammerlab.paths.Path
val reads = sc.loadBam(Path("test_bams/src/main/resources/2.bam"))  // RDD[SAMRecord]
reads.count  // Long: 4910
```

#### On Google Cloud

[spark-bam] uses Java NIO APIs to read files, and needs the [google-cloud-nio] connector in order to read from Google Cloud Storage (`gs://` URLs).

Download a shaded [google-cloud-nio] JAR:

```bash
GOOGLE_CLOUD_NIO_JAR=google-cloud-nio-0.20.0-alpha-shaded.jar
wget https://oss.sonatype.org/content/repositories/releases/com/google/cloud/google-cloud-nio/0.20.0-alpha/$GOOGLE_CLOUD_NIO_JAR
```

Then include it in your `--jars` list when running `spark-shell` or `spark-submit`:

```bash
spark-shell --jars $SPARK_BAM_JAR,$GOOGLE_CLOUD_NIO_JAR
…
import org.hammerlab.bam.spark._
import org.hammerlab.paths.Path
val reads = sc.loadBam(Path("gs://bucket/my.bam"))
```

## Benchmarks

### Accuracy

#### [spark-bam]

There are no known situations where [spark-bam] incorrectly classifies a BAM-record-boundary.

#### [hadoop-bam]

[hadoop-bam] seems to have a false-positive rate of about 1 in every TODO uncompressed BAM positions.
 
In all such false-positives:
- the true start of a read is one byte further. That read is:
	- unmapped, and
	- "placed" in chromosome 1 between TODO and TODO
- the "invalid cigar operator" and "empty read name" checks would both correctly rule out the position as a record-start

##### Data

Some descriptions of BAMs that have been used for benchmarking:

- DREAM challenge
	- synthetic dataset 2
		- normal BAM: 156.1GB, 22489 false-positives
		- tumor BAM: 173.1GB, 0 false-positives

### Speed

TODO

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

[`CanLoadBam`]: https://github.com/hammerlab/spark-bam/blob/master/load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala
[`loadReads`]: https://github.com/hammerlab/spark-bam/blob/master/load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#TODO
[`loadBamIntervals`]: https://github.com/hammerlab/spark-bam/blob/master/load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#TODO
[`loadReadsAndPositions`]: https://github.com/hammerlab/spark-bam/blob/master/load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#TODO
[`loadSplitsAndReads`]: https://github.com/hammerlab/spark-bam/blob/master/load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#TODO

[`LociSet`]: https://github.com/hammerlab/genomic-loci/blob/2.0.1/src/main/scala/org/hammerlab/genomics/loci/set/LociSet.scala

[`Pos`]: https://github.com/hammerlab/spark-bam/blob/master/bgzf/src/main/scala/org/hammerlab/bgzf/Pos.scala
[`Split`]: https://github.com/hammerlab/spark-bam/blob/master/check/src/main/scala/org/hammerlab/bam/spark/Split.scala

[linking]: #linking-against-spark-bam

[test_bams]: test_bams/src/main/resources
[cli/str/slice]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/resources/slice

[cli]: https://github.com/hammerlab/spark-bam/blob/master/cli
