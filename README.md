# spark-bam
[![Build Status](https://travis-ci.org/hammerlab/spark-bam.svg?branch=master)](https://travis-ci.org/hammerlab/spark-bam)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/spark-bam/badge.svg?branch=master)](https://coveralls.io/github/hammerlab/spark-bam?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/org.hammerlab/spark-bam_2.11.svg?maxAge=600)](http://search.maven.org/#search%7Cga%7C1%7Cspark-bam)

Load [BAM files][SAM spec] using [Apache Spark][] and [HTSJDK][]; inspired by [HadoopGenomics/hadoop-bam][hadoop-bam].

```scala
$ spark-shell --jars target/scala-2.11/spark-bam-assembly-1.1.0-SNAPSHOT.jar
…

import org.hammerlab.bam.spark._
import org.hammerlab.paths.Path

val path = Path("src/test/resources/5k.bam")

// Load an RDD[SAMRecord] from `path`; supports .bam, .sam, and .cram
sc.loadReads(path)

import org.hammerlab.bytes._

// Configure split size
sc.loadReads(path, splitSize = 16 MB)

// Return computed splits as well as the RDD[SAMRecord]
val BAMRecordRDD(splits, rdd) = sc.loadSplitsAndReads(path)
// splits: Seq[org.hammerlab.bam.spark.Split] = Vector(Split(2454:0,1010703:0))
// rdd: org.apache.spark.rdd.RDD[htsjdk.samtools.SAMRecord] = MapPartitionsRDD[6] at values at CanLoadBam.scala:200

// Example with multiple splits on a different BAM
val path = Path("src/test/resources/1.2203053-2211029.bam")
val BAMRecordRDD(splits, rdd) = sc.loadSplitsAndReads(path, splitSize = 400 KB)
// splits: … = Vector(Split(2454:0,416185:156), Split(416185:156,830784:124), Split(830784:124,963864:0))
// rdd: …
```

## Features

[spark-bam][] improves on [hadoop-bam][] in 3 ways:

- [parallelization](#Parallelization)
- [correctness](#Correctness)
- [algorithm/API clarity](#algorithm-api-clarity)

### Parallelization

[hadoop-bam][] computes splits sequentially on one node.

For BAMs in Google-Cloud Storage (GCS), it's been observed to take many minutes to compute splits for BAMs in the 10GB-100GB range. Two reasons for this slowness:
 
- GCS round-trips have high latency
- file-seek/-access patterns foil attempts to buffer data in the NIO/HDFS adapters for GCS access

[spark-bam][] identifies record-boundaries in each underlying file-split in the same Spark job that streams through the records, eliminating the driver-only bottleneck and maximally parallelizing the split-computation.

### Correctness

An important impetus for the creation of [spark-bam][] was the discovery of two TCGA lung-cancer BAMs for which [hadoop-bam][] produces invalid splits: [64-1681-01A-11D-2063-08.1.bam][1.bam] and [85-A512-01A-11D-A26M-08.6.bam][2.bam].

HTSJDK threw an error when trying to parse reads from essentially random data fed to it by [hadoop-bam][]:

```
MRNM should not be set for unpaired read
```

These BAMs were rendered unusable, and questions remained around whether such invalid splits could silently corrupt analyses.
 
#### Improved record-boundary-detection robustness

[spark-bam][] fixes these record-boundary-detection "false-positives" by adding additional checks:
  
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
| valid subsequent reads | 🚫† | ✅ |
| cigar consistent w/ seq len | 🚫 | 🚫 |


\* Cigar-op validity is not verified for the "record" that anchors a record-boundary candidate BAM position, but it is verified for the *subsequent* records that hadoop-bam checks

† Omitting this check has not resulted in any known spark-bam false-positives, but will likely be added at some point in the future anyway, as a precaution

#### Checking correctness

[spark-bam][] detects BAM-record boundaries using the pluggable [`Checker`][] interface.

Four implementations are provided:

##### [`eager`][]

Default/Production-worthy record-boundary-detection algorithm:

- includes [all the checks listed above][checks table]
- rules out a position as soon as any check fails

##### [`full`][]

Debugging-oriented [`Checker`][]:

- runs [all the checks listed above][checks table]
- emits information on all checks that passed or failed at each position
    - useful for downstream analysis of the accuracy of individual checks or subsets of checks
    - see [the `full-check` command](#TODO) or its stand-alone "main" app at [`org.hammerlab.bam.check.full.Main`](#TODO)
    - see [sample output in tests](src/test/scala/org/hammerlab/bam/check/full/MainTest.scala#L276-L328)

##### [`seqdoop`][]

[`Checker`][] that mimicks [hadoop-bam][]'s [`BAMSplitGuesser`][] as closely as possible.

- Useful for analyzing [hadoop-bam][]'s correctness
- Uses the [hammerlab/hadoop-bam][] fork, which exposes [`BAMSplitGuesser`][] logic more efficiently/directly
 
### Algorithm/API clarity

Analyzing [hadoop-bam][]'s correctness ([as discussed above](#seqdoop)) proved quite difficult due to subtleties in its implementation. 

Its record-boundary-detection is sensitive, in terms of both output and runtime, to:

- position within a BGZF block
- arbitrary (256KB) buffer size
- [_JVM heap size_](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L212) (!!! 😱)

[spark-bam][]'s accuracy is dramatically easier to reason about:

- buffer sizes are irrelevant
- OOMs are neither expected nor depended on for correctness
- file-positions are evaluated hermetically

This allows for greater confidence in the correctness of computed splits and downstream analyses.
  
#### Case study: counting on OOMs

While evaluating [hadoop-bam][]'s correctness, BAM positions were discovered that [`BAMSplitGuesser`][] would correctly deem as invalid *iff the JVM heap size was **below** a certain threshold*; larger heaps would avoid an OOM and mark an invalid position as valid.

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

## Using [spark-bam][]

### From `spark-shell`

After [getting an assembly JAR][]:

#### Locally

```scala
spark-shell --jars $SPARK_BAM_JAR
…
import org.hammerlab.bam.spark._
import org.hammerlab.paths.Path
val reads = sc.loadBam(Path("src/test/resources/5k.bam"))  // RDD[SAMRecord]
reads.count  // Long: 4910
```

#### On Google Cloud

[spark-bam][] uses Java NIO APIs to read files, and needs the [google-cloud-nio][] connector in order to read from Google Cloud Storage (GCS).

Download a shaded [google-cloud-nio][] JAR:

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

### Command-line interface

[spark-bam][] includes several standalone apps as subcommands:

#### Spark apps
- [`check-bam`][]
- [`full-check`][]
- [`compute-splits`][]
- [`compare-splits`][]

#### Single-node / Non-Spark apps
- [`index-blocks`][]
- [`index-records`][]
- [`htsjdk-rewrite`][]

#### Running a subcommand
```bash
spark-submit <spark confs> $SPARK_BAM_JAR <subcommand> <options>
```

##### Example
To compare spark-bam and hadoop-bam on all positions in a local BAM:

```bash
spark-submit $SPARK_BAM_JAR check-bam src/test/resources/5k.bam
…
3139404 uncompressed positions
987K compressed
Compression ratio: 3.11
4910 reads
All calls matched!
```

##### Help / Usage
All commands support running with `-h`/`--help`, for example:

```bash
$ spark-submit $SPARK_BAM_JAR check-bam -h
Command: check-bam
Usage: default-base-command check-bam
  --bgzf-block-headers-to-check | -z  <num>
        When searching for BGZF-block boundaries, look this many blocks ahead to verify that a candidate is a valid block. In general, probability of a false-positive is 2^(-32N) for N blocks of look-ahead
  --ranges | --intervals | -i  <intervals>
        Comma-separated list of byte-ranges to restrict computation to; when specified, only BGZF blocks whose starts are in this set will be considered. Allowed formats: <start>-<end>, <start>+<length>, <position>. All values can take integer values or byte-size shorthands (e.g. "10m")
  --blocks-path | -b  <path>
        File with bgzf-block-start positions as output by index-blocks; If unset, the BAM path with a ".blocks" extension appended is used. If this path doesn't exist, use a parallel search for BGZF blocks (see --bgzf-block-headers-to-check)
  --split-size | -m  <bytes>
        Maximum Hadoop split-size; if unset, default to underlying FileSystem's value. Integers as well as byte-size short-hands accepted, e.g. 64m, 32MB
  --records-path | -r  <path>
        File with BAM-record-start positions, as output by index-records. If unset, the BAM path with a ".records" extension appended is used
  --warn
        Set the root logging level to WARN; useful for making Spark display the console progress-bar in client-mode
  --print-limit | -l  <num>
        When collecting samples of records/results for displaying to the user, limit to this many to avoid overloading the driver
  --output-path | -o  <path>
        Print output to this file, otherwise to stdout
  --results-per-partition | -p  <num>
        After running eager and/or seqdoop checkers over a BAM file and filtering to just the contested positions, repartition to have this many records per partition. Typically there are far fewer records at this stage, so it's useful to coalesce down to avoid 1,000's of empty partitions
  --spark-bam | -s
        Run the spark-bam checker; if both or neither of -s and -u are set, then they are both run, and the results compared. If only one is set, its results are compared against a "ground truth" file generated by the index-records command
  --hadoop-bam | --upstream | -u
        Run the hadoop-bam checker; if both or neither of -s and -u are set, then they are both run, and the results compared. If only one is set, its results are compared against a "ground truth" file generated by the index-records command
```

#### [`compute-splits`][spark/Main]: compute/compare splits

Test and time split computation in spark-bam and/or hadoop-bam.

Example usage highlighting a hadoop-bam bug on a local/test BAM:
 
```bash
$ spark-submit \
	$SPARK_BAM_JAR \
	compute-splits \
	-m 470k \
	src/test/resources/1.2203053-2211029.bam
…
2 splits differ (totals: 2, 2):
		486847:6-963864:65535
	486847:7-963864:0
```

The BAM `1.2203053-2211029.bam` is a ≈1MB excerpt from the TCGA BAM [64-1681-01A-11D-2063-08.1.bam][1.bam], where hadoop-bam picks an invalid split from offset 64MB.
 
The 470KB split-size used above drops hadoop-bam into the same position in the excerpted BAM, reproducing the bug in a smaller test-case. 

#### [`compare-splits`][compare/Main]: compare spark-bam and hadoop-bam splits on multiple/many BAMs

Similar to [`compute-splits`][], but takes an `-f` parameter pointing to a file with many BAM paths, and compares spark-bam's and hadoop-bam's computed splits on all of them (or a subset given by `-s`/`-n` flags).



#### [check/Main][]: evaluate a [`Checker`][]

This app will run over every (uncompressed) position in a BAM file, applying one of the three [`Checker`][]s ([`eager`][], [`full`][], [`seqdoop`][]) and printing statistics about its performance; see [sample output for the `full` checker above](#full).

**Before running:** the BAM file must have its BGZF blocks and "virtual" record-start-positions indexed with [IndexBlocks](#IndexBlocks) and [IndexRecords](#IndexRecords).

Then, [check/Main][] can be run:

```bash
spark-submit \
	--class org.hammerlab.bam.check.Main $SPARK_BAM_JAR \
	<options> \
	<path to BAM>
```

Example `<options>`:
- `-n 32`: use 32 threads on the driver
	- default/none: compute splits with a Spark job, one split per task
- `-e`/`-f`/`-s`: evaluate the [`eager`][], [`full`][], or [`seqdoop`][] checker

See [the arguments definition](#TODO) for information about other available options. 

#### [IndexRecords][]

Outputs a `.bam.records` file with "virtual offsets" of all BAM records in a `.bam` file; see [the test data](src/test/resources) or [`IndexRecordsTest`][] for example output:

```
2454,0
2454,624
2454,1244
2454,1883
2454,2520
2454,3088
2454,3734
2454,4368
2454,4987
…
```

##### Example usage

```bash
spark-submit \
	--class org.hammerlab.bam.index.IndexRecords \
	$SPARK_BAM_JAR \
	<input .bam>
```

#### [IndexBlocks][]

Outputs a `.bam.blocks` file with {start position, compressed size, and uncompressed size} for each BGZF block in an input `.bam` file; see [the test data](src/test/resources) or [`IndexBlocksTest`][] for example output:

```
0,2454,5650
2454,25330,65092
27784,23602,64902
51386,25052,65248
76438,21680,64839
98118,20314,64643
118432,19775,65187
138207,20396,64752
158603,21533,64893
…
```

##### Example usage

```bash
spark-submit \
	--class org.hammerlab.bgzf.index.IndexBlocks \
	$SPARK_BAM_JAR \
	<input .bam>
```

### As a library

#### Depend on [spark-bam][] from Maven

```xml
<dependency>
	<groupId>org.hammerlab</groupId>
	<artifactId>spark-bam_2.11</artifactId>
	<version>1.1.0-SNAPSHOT</version>
</dependency>
```

#### Depend on [spark-bam][] from SBT

```scala
libraryDependencies += "org.hammerlab" %% "spark-bam" % "1.1.0-SNAPSHOT"
```

### Get an assembly JAR

#### From Maven Central:

```
wget TODO
```

#### From source

```
git clone git@github.com:hammerlab/spark-bam.git
cd spark-bam
sbt assembly
export SPARK_BAM_JAR=target/scala-2.11/spark-bam-assembly-1.1.0-SNAPSHOT.jar
```


[checks table]: #improved-record-boundary-detection-robustness
[getting an assembly JAR]: #get-an-assembly-JAR

[`eager`]: src/main/scala/org/hammerlab/bam/check/eager/Checker.scala
[`full`]: src/main/scala/org/hammerlab/bam/check/full/Checker.scala
[`seqdoop`]: src/main/scala/org/hammerlab/bam/check/seqdoop/Checker.scala

[`Checker`]: src/main/scala/org/hammerlab/bam/check/Checker.scala
[`BAMSplitGuesser`]: https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java

[Apache Spark]: https://spark.apache.org/
[HTSJDK]: https://github.com/samtools/htsjdk
[Google Cloud Dataproc]: https://cloud.google.com/dataproc/
[bigdata-interop]: https://github.com/GoogleCloudPlatform/bigdata-interop/
[google-cloud-nio]: https://github.com/GoogleCloudPlatform/google-cloud-java/tree/v0.10.0/google-cloud-contrib/google-cloud-nio
[SAM spec]: http://samtools.github.io/hts-specs/SAMv1.pdf

[hadoop-bam]: https://github.com/HadoopGenomics/Hadoop-BAM
[spark-bam]: https://github.com/hammerlab/spark-bam
[hammerlab/hadoop-bam]: https://github.com/hammerlab/Hadoop-BAM/tree/7.9.0

[Main]: src/main/scala/org/hammerlab/bam/Main.scala

[`check-bam`]: #check
[check/Main]: src/main/scala/org/hammerlab/bam/check/Main.scala

[`full-check`]: #full-check
[full/Main]: src/main/scala/org/hammerlab/bam/check/full/Main.scala

[`compute-splits`]: #compute-splits
[spark/Main]: src/main/scala/org/hammerlab/bam/spark/Main.scala

[`compare-splits`]: #compare-splits
[compare/Main]: src/main/scala/org/hammerlab/bam/compare/Main.scala

[`index-blocks`]: #index-blocks
[IndexBlocks]: src/main/scala/org/hammerlab/bgzf/index/IndexBlocks.scala
[`IndexBlocksTest`]: src/test/scala/org/hammerlab/bgzf/index/IndexBlocksTest.scala

[`index-records`]: #index-records
[IndexRecords]: src/main/scala/org/hammerlab/bam/index/IndexRecords.scala
[`IndexRecordsTest`]: src/test/scala/org/hammerlab/bam/index/IndexRecordsTest.scala

[`htsjdk-rewrite`]: #htsjdk-rewrite
[rewrite/Main]: src/main/scala/org/hammerlab/bam/rewrite/Main.scala

[1.bam]: https://portal.gdc.cancer.gov/cases/c583fdd1-8cd2-4c15-a23e-0644261f65da?bioId=3813c301-9622-4567-bc72-d17acbeb236f
[2.bam]: https://portal.gdc.cancer.gov/cases/bcb6447f-3e4c-44fe-afc3-100d5dbe9ba2?bioId=f62f6ba1-59a7-4b16-b69f-15fa3dabfbc1
