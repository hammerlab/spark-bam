# spark-bam
[![Build Status](https://travis-ci.org/hammerlab/spark-bam.svg?branch=master)](https://travis-ci.org/hammerlab/spark-bam)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/spark-bam/badge.svg?branch=master)](https://coveralls.io/github/hammerlab/spark-bam?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/org.hammerlab/spark-bam_2.11.svg?maxAge=600)](http://search.maven.org/#search%7Cga%7C1%7Cspark-bam)

Load [BAM files](http://samtools.github.io/hts-specs/SAMv1.pdf) using [Apache Spark](https://spark.apache.org/) and [HTSJDK](https://github.com/samtools/htsjdk):

```scala
import org.hammerlab.spark.bam._
import org.hammerlab.hadoop.Path

// Make SparkContext implicitly available
implicit val sparkContext = sc

val path = Path("src/test/resources/5k.bam")

val reads = sc.loadBam(path)  // Default: use [4x the available cores] threads on the driver to compute splits

reads.count  // res1: Long = 4910
```

Inspired by [HadoopGenomics/hadoop-bam][hadoop-bam].

## Features

[spark-bam][] improves on [hadoop-bam][] in 3 ways:

- [parallelization](#Parallelization)
- [correctness](#Correctness)
- [algorithm/API clarity](#algorithm-api-clarity)

### Parallelization

[hadoop-bam][] computes splits sequentially on one node; in recent benchmarks this took [2 (resp. 15) minutes on a 20GB (resp. 178GB) BAM](#TODO) in Google-Cloud Storage (GCS).

[spark-bam][] offers two methods for parallelizing this that each improve latency by a factor of XXX:

TODO: scaling figure

#### Parallelization strategy #1: threads on the driver

![Depcition of a driver machine analyzing split starts in sequence vs. in parallel](https://cl.ly/3E3p1c2J2q2W/Screen%20Shot%202017-06-28%20at%2011.09.55%20AM.png)

Using N threads to parallelize fetching the start of each `FileInputFormat`-split [exhibits near-perfect linear scaling (up to 64 threads on a 4-core GCE `n1-standard-4` VM)](#TODO)), bringing split-computation from minutes to seconds:

TODO: scaling figure

##### Example usage:

```scala
implicit val parallelizer = org.hammerlab.parallel.Threads(32)

sc.loadBam(path)
```

#### Parallelization strategy #2: Spark job

For especially largs BAMs, parallelizing across many machines can be worthwhile: 

![Depiction of using a Spark job to compute split starts](https://cl.ly/2k0B3p3o3H3L/Screen%20Shot%202017-06-28%20at%2011.15.03%20AM.png)

Benchmarks suggest this approach is beneficial for BAMs [greater than TODO in size](TODO):
 
TOGO: scaling figure

##### Example usage:

By default, one file-split is processed by each Spark partition:

```scala
import org.hammerlab.parallel.Spark

implicit val parallelizer = Spark

sc.loadBam(path)  
```

Alternately, set the number of splits to compute in each Spark task:

```scala
// Contains Spark-partitioning configurations
import org.hammerlab.parallel.spark._

sc.loadBam(path)(Spark(ElemsPerPartition(10)))
```

Or, fix the number of partitions:

```scala
sc.loadBam(path)(Spark(NumPartitions(100)))
```

### Correctness
An important impetus for the creation of [spark-bam][] was the discovery of two TCGA lung-cancer BAMs for which [hadoop-bam][] produces invalid splits: [64-1681-01A-11D-2063-08.1.bam](https://portal.gdc.cancer.gov/cases/c583fdd1-8cd2-4c15-a23e-0644261f65da?bioId=3813c301-9622-4567-bc72-d17acbeb236f) and [85-A512-01A-11D-A26M-08.6.bam](https://portal.gdc.cancer.gov/cases/bcb6447f-3e4c-44fe-afc3-100d5dbe9ba2?bioId=f62f6ba1-59a7-4b16-b69f-15fa3dabfbc1).

HTSJDK threw an error downstream when trying to parse reads from essentially random data:

```
MRNM should not be set for unpaired read
```

These BAMs were rendered unusable, and questions remained around whether such invalid splits could silently corrupt analyses.
 
#### Improved record-boundary-detection robustness

[spark-bam][] fixes these record-boundary-detection "false-positives" by adding additional checks to the heuristic:
  
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
| cigar ops valid | ✅ | ✅ |
| valid subsequent reads | 🚫 | ✅ |
| cigar consistent w/ seq len | 🚫 | 🚫 |

#### [`Checker`][] interface

[spark-bam][] detects BAM-record boundaries using the pluggable [`Checker`][] interface.

Three implementations are provided:

##### [Eager][`eager`]

Default/Production-worthy record-boundary-detection algorithm:

- includes [all the checks listed above][checks table]
- rules out a position as soon as any check fails

##### [Full][`full`]

Debugging-oriented [`Checker`][]:

- runs [all the checks listed above][checks table]
- emits information on all checks that passed or failed at each position
    - useful for downstream analysis of the accuracy of individual checks or subsets of checks
    - see [check/Main's "full mode"][check/Main] and [full/Run][]; sample output:
          
        ```
        29890275308 positions checked (94203894 reads), no errors!
        Critical error counts (true negatives where only one check failed):
              tooFewFixedBlockBytes:    35
          nonNullTerminatedReadName:    11
        
        True negatives where exactly two checks failed:
                     invalidCigarOp:    4201704
                         noReadName:    2409311
          nonNullTerminatedReadName:    1304621
                      emptyReadName:     255322
                tooLargeNextReadIdx:     204156
                   nonASCIIReadName:      37088
                    tooLargeReadIdx:      10820
        tooFewRemainingBytesImplied:      10586
                    negativeReadIdx:          2
                    negativeReadPos:          2
        
        Total error counts:
                     invalidCigarOp:    28661374692
                tooLargeNextReadIdx:    27924049452
                    tooLargeReadIdx:    27924049452
          nonNullTerminatedReadName:    24885666031
        tooFewRemainingBytesImplied:    23071387740
                   nonASCIIReadName:     2367016056
                         noReadName:     2271887125
                negativeNextReadIdx:     1582430053
                    negativeReadIdx:     1582430053
                    negativeReadPos:     1582430053
                negativeNextReadPos:     1582430053
                      emptyReadName:      232401822
                tooLargeNextReadPos:       43095171
                    tooLargeReadPos:       43095171
             tooFewBytesForReadName:             73
              tooFewFixedBlockBytes:             35
             tooFewBytesForCigarOps:             16
        ```

##### [Seqdoop][`seqdoop`]

[`Checker`][] that mimicks [hadoop-bam][]'s [`BAMSplitGuesser`][] as closely as possible.

- Useful for analyzing [hadoop-bam][]'s correctness
- Uses the [hammerlab/hadoop-bam][] fork, which exposes [`BAMSplitGuesser`][] logic more efficiently/directly
 
### Algorithm/API clarity

Analyzing [hadoop-bam][]'s correctness ([as discussed above](#seqdoop)) proved quite difficult due to subtleties in [hadoop-bam][]'s implementation.

Its record-boundary-detection is sensitive, in terms of both output and runtime, to:

- position within a BGZF block
- arbitrary (256KB) buffer size
- [_JVM heap size_](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L212) (!!! 😱)

[spark-bam][]'s accuracy is dramatically easier to reason about:

- buffer sizes are irrelevant
- OOMs are neither expected nor depended on for correctness
- file-positions are evaluated basically hermetically

This allows for greater confidence in the correctness of computed splits and downstream analyses.
  
#### Case study: counting on OOMs

An unsettling behavior discovered while evaluating [hadoop-bam][]'s correctness was the existence of positions in BAMs that [`BAMSplitGuesser`][] would correctly deem as invalid *iff the JVM heap size was below a certain threshold*.

While evaluating such a position:

- [the initial check of the validity of a potential record starting at that position](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L168) would pass
- a check of that and subsequent records, primarily focused on validity of cigar operators and proceeding until at least 3 distinct BGZF block positions had been visited, [would commence](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L183)
- the first record, already validated to some degree, would pass the cigar-operator-validity check, and [the `decodedAny` flag would be set to `true`](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L190)
- [HTSJDK's `BAMRecordCodec is asked to decode](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L185) the next "record" (in actuality just gibberish data from somewhere in the middle of a true record)
- records always begin with a 4-byte integer indicating how many bytes long they are
- in these cases, we get a large integer, say ≈1e9, implying the next record is ≈1GB long
- [`BAMRecordCodec` attempts to allocate a byte-array of that size](https://github.com/samtools/htsjdk/blob/2.9.1/src/main/java/htsjdk/samtools/BAMRecordCodec.java#L198) and read the "record" into it
	- if the allocation succeeds:
		- a `RuntimeEOFException` is thrown while attempting to read ≈1GB of data from a buffer that is [only ≈256KB in size](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L126-L144)
		- [this exception is caught, and the `decodedAny` flag signals that this position is valid](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L224-L229) because at least one record was decoded before "EOF" (which actually only represents an "end of 256KB buffer") occurred
		- the position is not actually valid! 💥🚫😱
	- if the allocation fails, [an OOM is caught and taken to signal that this is not a valid record position](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L212) (which is true!)

This resulted in positions that hadoop-bam correctly ruled out in sufficiently-memory-constrained test-contexts, but false-positived on in more-generously-provisioned settings, which is obviously a very undesirable relationship to correctness.  

## Using [spark-bam][]

### From `spark-shell`

After [getting an assembly JAR][]:

#### Locally

```scala
spark-shell --jars $SPARK_BAM_JAR
…

import org.hammerlab.bam.spark._
import org.hammerlab.hadoop.Path

val reads = sc.loadBam(Path("src/test/resources/5k.bam"))  // Default: use [4x the available cores] threads on the driver to compute splits
reads.count
```

#### On Google Cloud

On [Google Cloud Dataproc][] nodes, [spark-bam][] should read BAMs from GCS automatically, thanks to [Google's HDFS↔GCS bridge][bigdata-interop].

##### [google-cloud-nio][]
Some [spark-bam][] functions use Java NIO filesystem APIs, and should be provided with a [google-cloud-nio][] shaded JAR in order to read from `gs://` URLs:

```bash
GOOGLE_CLOUD_NIO_JAR=google-cloud-nio-0.20.0-alpha-shaded.jar
wget https://oss.sonatype.org/content/repositories/releases/com/google/cloud/google-cloud-nio/0.20.0-alpha/$GOOGLE_CLOUD_NIO_JAR
```

Then run `spark-shell` like before, including the `google-cloud-nio` JAR:

```scala
spark-shell --jars $SPARK_BAM_JAR,$GOOGLE_CLOUD_NIO_JAR
…

import org.hammerlab.spark.bam._
import org.hammerlab.hadoop.Path

val reads = sc.loadBam(Path("gs://bucket/my.bam"))
```

### Standalone apps

[spark-bam][] bundles several standalone apps:

#### [spark/Main][]: compute splits

This app allows testing and timing split computation with various configurations:
 
```bash
spark-submit \
	--class org.hammerlab.bam.spark.Main \
	$SPARK_BAM_JAR \
	<options>
	<path to BAM> 

```

`<options>` can include:
- `-d`: compute splits using [hadoop-bam][] (default uses [spark-bam][])
- `-c`: compare splits computed by [hadoop-bam][] and [spark-bam][]
- `-m`: maximum split size (e.g. `32m`; default: HDFS block size or `64m`)
- `-n`: when computing splits with [spark-bam][], use ["threads mode"][] (default: ["spark mode"][])
- `-r`: print the count of reads on each computed partition (also useful to verify that legitimate splits were found)

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


[hadoop-bam]: https://github.com/HadoopGenomics/Hadoop-BAM
[spark-bam]: https://github.com/hammerlab/spark-bam
[`Checker`]: src/main/scala/org/hammerlab/bam/check/Checker.scala
[checks table]: #improved-record-boundary-detection-robustness
[check/Main]: src/main/scala/org/hammerlab/bam/check/Main.scala
[full/Run]: src/main/scala/org/hammerlab/bam/check/full/Run.scala
[`BAMSplitGuesser`]: https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java
[hammerlab/hadoop-bam]: #TODO
[spark/Main]: src/main/scala/org/hammerlab/bam/spark/Main.scala
[getting an assembly JAR]: #get-an-assembly-JAR
[google-cloud-nio]: https://github.com/GoogleCloudPlatform/google-cloud-java/tree/v0.10.0/google-cloud-contrib/google-cloud-nio
[Google Cloud Dataproc]: https://cloud.google.com/dataproc/
[IndexBlocks]: src/main/scala/org/hammerlab/bgzf/index/IndexBlocks.scala
[IndexRecords]: src/main/scala/org/hammerlab/bam/index/IndexRecords.scala
[`eager`]: src/main/scala/org/hammerlab/bam/check/eager/Checker.scala
[`full`]: src/main/scala/org/hammerlab/bam/check/full/Checker.scala
[`seqdoop`]: src/main/scala/org/hammerlab/bam/check/seqdoop/Checker.scala
[`IndexRecordsTest`]: src/test/scala/org/hammerlab/bam/index/IndexRecordsTest.scala
[`IndexBlocksTest`]: src/test/scala/org/hammerlab/bgzf/index/IndexBlocksTest.scala
[bigdata-interop]: https://github.com/GoogleCloudPlatform/bigdata-interop/
["threads mode"]: #parallelization-strategy-1-threads-on-the-driver
["spark mode"]: #parallelization-strategy-2-spark-job
