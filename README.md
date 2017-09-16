# spark-bam
[![Build Status](https://travis-ci.org/hammerlab/spark-bam.svg?branch=master)](https://travis-ci.org/hammerlab/spark-bam)

Process [BAM files][SAM spec] using [Apache Spark] and [HTSJDK]; inspired by [hadoop-bam].

```scala
$ spark-shell --packages=org.hammerlab.bam:load:1.0.0-SNAPSHOT
…
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

## Features

[spark-bam] improves on [hadoop-bam] in 3 ways:

- [parallelization][parallelization-section]
- [correctness][correctness-section]
- [algorithm/API clarity][algorithm-clarity-section]

### Parallelization

[hadoop-bam] computes splits sequentially on one node. Depending on the storage backend, this can take many minutes for modest-sized (10-100GB) BAMs, leaving a large cluster idling while the driver bottlenecks on an emminently-parallelizable task.
 
For example, on Google Cloud Storage (GCS), two factors causing high split-computation latency include:

- high GCS round-trip latency
- file-seek/-access patterns that nullify buffering in GCS NIO/HDFS adapters

[spark-bam] identifies record-boundaries in each underlying file-split in the same Spark job that streams through the records, eliminating the driver-only bottleneck and maximally parallelizing split-computation.

### Correctness

An important impetus for the creation of [spark-bam] was the discovery of two TCGA lung-cancer BAMs for which [hadoop-bam] produces invalid splits:

- [19155553-8199-4c4d-a35d-9a2f94dd2e7d]
- [b7ee4c39-1185-4301-a160-669dea90e192]

HTSJDK threw an error when trying to parse reads from essentially random data fed to it by [hadoop-bam]:

```
MRNM should not be set for unpaired read
```

These BAMs were rendered unusable, and questions remain around whether such invalid splits could silently corrupt analyses.
 
#### Improved record-boundary-detection robustness

[spark-bam] fixes these record-boundary-detection "false-positives" by adding additional checks:
  
| Validation check | spark-bam | hadoop-bam |
| --- | --- | --- |
| negative ref idx | ✅ | ✅ |
| ref idx too large | ✅ | ✅ |
| negative locus | ✅ | ✅ |
| locus too large | ✅ | 🚫 |
| read-name ends w/ `\0` | ✅ | ✅ |
| read-name (incl. `\0`) non-empty | ✅ | ✅ |
| read-name non-empty | ✅ | 🚫 |
| invalid read-name chars | ✅ | 🚫 |
| record length consistent w/ #{bases, cigar ops} | ✅ | ✅ |
| cigar ops valid | ✅ | 🌓* |
| valid subsequent reads | ✅ | ✅ |
| non-empty cigar/seq in mapped reads | ✅ | 🚫 |
| cigar consistent w/ seq len | 🚫 | 🚫 |


\* Cigar-op validity is not verified for the "record" that anchors a record-boundary candidate BAM position, but it is verified for the *subsequent* records that hadoop-bam checks

#### Checking correctness

[spark-bam] detects BAM-record boundaries using the pluggable [`Checker`] interface.

Four implementations are provided:

##### [`eager`][eager/Checker]

Default/Production-worthy record-boundary-detection algorithm:

- includes [all the checks listed above][checks table]
- rules out a position as soon as any check fails
- can be compared against [hadoop-bam]'s checking logic (represented by the [`seqdoop`] checker) using the [`check-bam`], [`compute-splits`], and [`compare-splits`] commands
- used in BAM-loading APIs exposed to downstream libraries

##### [`full`][full/Checker]

Debugging-oriented [`Checker`]:

- runs [all the checks listed above][checks table]
- emits information on all checks that passed or failed at each position
    - useful for downstream analysis of the accuracy of individual checks or subsets of checks
    - see [the `full-check` command][`full-check`] or its stand-alone "main" app at [`org.hammerlab.bam.check.full.Main`][full/Main]
    - see [sample output in tests](src/test/scala/org/hammerlab/bam/check/full/MainTest.scala#L276-L328)

##### [`seqdoop`]

[`Checker`] that mimicks [hadoop-bam]'s [`BAMSplitGuesser`] as closely as possible.

- Useful for analyzing [hadoop-bam]'s correctness
- Uses the [hammerlab/hadoop-bam] fork, which exposes [`BAMSplitGuesser`] logic more efficiently/directly

##### [`indexed`][indexed/Checker]

This [`Checker`] simply reads from a `.records` file (as output by [`index-records`]) and reflects the read-positions listed there.
 
It can serve as a "ground truth" against which to check either the [`eager`] or [`seqdoop`] checkers (using the `-s` or `-u` flags to [`check-bam`], resp.).

#### Future-proofing

Some assumptions in [hadoop-bam] are likely to break when processing long reads.
 
For example, a 100kbp-long read is likely to span multiple BGZF blocks, likely causing [hadoop-bam] to reject it as invalid.

It is believed that [spark-bam] will be robust to such situations, related to [its agnosticity about buffer-sizes / reads' relative positions with respect to BGZF-block boundaries][api-clarity], though this has not been tested.


### Algorithm/API clarity

Analyzing [hadoop-bam]'s correctness ([as discussed above](#seqdoop)) proved quite difficult due to subtleties in its implementation. 

Its record-boundary-detection is sensitive, in terms of both output and runtime, to:

- position within a BGZF block
- arbitrary (256KB) buffer size
- [_JVM heap size_](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L212) (!!! 😱)

[spark-bam]'s accuracy is dramatically easier to reason about:

- buffer sizes are irrelevant
- OOMs are neither expected nor depended on for correctness
- file-positions are evaluated hermetically

This allows for greater confidence in the correctness of computed splits and downstream analyses.
  
#### Case study: counting on OOMs

While evaluating [hadoop-bam]'s correctness, BAM positions were discovered that [`BAMSplitGuesser`] would correctly deem as invalid *iff the JVM heap size was **below** a certain threshold*; larger heaps would avoid an OOM and mark an invalid position as valid.

An overview of this failure mode:

- [the initial check of the validity of a potential record starting at that position](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L168) would pass
- a check of that and subsequent records, primarily focused on validity of cigar operators and proceeding until at least 3 distinct BGZF block positions had been visited, [would commence](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L183)
- the first record, already validated to some degree, would pass the cigar-operator-validity check, and [the `decodedAny` flag would be set to `true`](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L190)
- [HTSJDK's `BAMRecordCodec is asked to decode](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L185) the next "record" (in actuality just gibberish data from somewhere in the middle of a true record)
- records always begin with a 4-byte integer indicating how many bytes long they are
- in these cases, we get a large integer, say ≈1e9, implying the next "record" is ≈1GB long
- [`BAMRecordCodec` attempts to allocate a byte-array of that size](https://github.com/samtools/htsjdk/blob/2.9.1/src/main/java/htsjdk/samtools/BAMRecordCodec.java#L198) and read the "record" into it
	- if the allocation succeeds:
		- a `RuntimeEOFException` is thrown while attempting to read ≈1GB of data from a buffer that is [only ≈256KB in size](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L126-L144)
		- [this exception is caught, and the `decodedAny` flag signals that this position is valid](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L224-L229) because at least one record was decoded before "EOF" (which actually only represents an "end of 256KB buffer") occurred
		- the position is not actually valid! 💥🚫😱
	- if the allocation fails, [an OOM is caught and taken to signal that this is not a valid record position](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L212) (which is true!)

This resulted in positions that hadoop-bam correctly ruled out in sufficiently-memory-constrained test-contexts, but false-positived on in more-generously-provisioned settings, which is obviously an undesirable relationship to correctness.

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

<!-- External BAM links -->
[19155553-8199-4c4d-a35d-9a2f94dd2e7d]: https://portal.gdc.cancer.gov/legacy-archive/files/19155553-8199-4c4d-a35d-9a2f94dd2e7d
[b7ee4c39-1185-4301-a160-669dea90e192]: https://portal.gdc.cancer.gov/legacy-archive/files/b7ee4c39-1185-4301-a160-669dea90e192

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

[parallelization-section]: #parallelization
[correctness-section]: #correctness
[algorithm-clarity-section]: #algorithmapi-clarity
