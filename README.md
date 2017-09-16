# spark-bam
[![Build Status](https://travis-ci.org/hammerlab/spark-bam.svg?branch=master)](https://travis-ci.org/hammerlab/spark-bam)

Process [BAM files][SAM spec] using [Apache Spark] and [HTSJDK]; inspired by [HadoopGenomics/hadoop-bam][hadoop-bam].

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

- [parallelization](#Parallelization)
- [correctness](#Correctness)
- [algorithm/API clarity](#algorithm-api-clarity)

### Parallelization

[hadoop-bam] computes splits sequentially on one node. Depending on the storage backend, this can take many minutes for modest-sized (10-100GB) BAMs, leaving a large cluster idling while the driver bottlenecks on an emminently-parallelizable task.
 
For example, on Google Cloud Storage (GCS), two factors causing high split-computation latency include:

- high GCS round-trip latency
- file-seek/-access patterns that nullify buffering in GCS NIO/HDFS adapters

[spark-bam] identifies record-boundaries in each underlying file-split in the same Spark job that streams through the records, eliminating the driver-only bottleneck and maximally parallelizing split-computation.

### Correctness

An important impetus for the creation of [spark-bam] was the discovery of two TCGA lung-cancer BAMs for which [hadoop-bam] produces invalid splits:

- [19155553-8199-4c4d-a35d-9a2f94dd2e7d][1.bam]
- [b7ee4c39-1185-4301-a160-669dea90e192][2.bam]

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

### Command-line interface

[spark-bam's cli module][cli] includes several standalone apps as subcommands:

#### Spark apps
- [`check-bam`]
- [`full-check`]
- [`compute-splits`]
- [`compare-splits`]

#### Single-node / Non-Spark apps
- [`index-blocks`]
- [`index-records`]
- [`htsjdk-rewrite`]

#### Setup

Download a CLI assembly JAR from Maven Central:

```bash
wget -O spark-bam-cli.jar https://oss.sonatype.org/content/repositories/snapshots/org/hammerlab/bam/cli_2.11/1.0.0-SNAPSHOT/cli_2.11-1.0.0-SNAPSHOT-assembly.jar
export CLI_JAR=spark-bam-cli.jar
```

Or build one from source:

```bash
sbt cli/assembly
export CLI_JAR=cli/target/scala-2.11/cli-assembly-1.0.0-SNAPSHOT.jar
```

#### Running a subcommand

```bash
spark-submit <spark confs> $CLI_JAR <subcommand> <options>
```

##### Example
Run [`check-bam`] to compare spark-bam and hadoop-bam on all positions in a local BAM:

```bash
spark-submit $CLI_JAR check-bam test_bams/src/main/resources/2.bam
…
1606522 uncompressed positions
519K compressed
Compression ratio: 3.02
2500 reads
All calls matched!
```

##### Help / Usage
All commands support running with `-h`/`--help`, for example:

```bash
$ spark-submit $CLI_JAR compare-splits -h
Command: compare-splits
Usage: default-base-command compare-splits
  --print-limit | -l  <num=1000000>
        When collecting samples of records/results for displaying to the user, limit to this many to avoid overloading the driver
  --output-path | -o  <path>
        Print output to this file, otherwise to stdout
  --split-size | --max-split-size | -m  <bytes>
        Maximum Hadoop split-size; if unset, default to underlying FileSystem's value. Integers as well as byte-size short-hands accepted, e.g. 64m, 32MB
  --files-limit | -n  <value>
        Only process this many files
  --start-offset | -s  <value>
        Start from this offset into the file
```

##### Other common arguments / options

###### Required argument: `<path>`
Most commands require exactly one argument: a path to operate on in some way.

This is usually a BAM file, but in the case of [`compare-splits`] is a file with lists of BAM-file paths.

###### Output path
They also take an optional "output path" via `--output-path` or `-o`, which they write their meaningful output to; if not set, stdout is used.

```
--output-path | -o  <path>
      Print output to this file, otherwise to stdout
```

###### Print limit
Commands' output is also governed by the "print-limit" (`--print-limit` / `-l`) option.

Various portions of output data will be capped to this number of records/lines to avoid overloading the Spark driver with large `collect` jobs in cases where a prodigious amount of records match some criteria (e.g. false-positive positions in a [`check-bam`] run).

```
--print-limit | -l  <num=1000000>
      When collecting samples of records/results for displaying to the user, limit to this many to the user, limit to this many to avoid overloading the driver
```

###### `--split-size` / `-m`
Many commands are sensitive to the split-size. Default is often taken from the underlying Hadoop FileSystem, though in e.g. `check-bam` it is set to 2MB to keep the work per Spark-task manageable.

```
--split-size | --max-split-size | -m  <bytes>
      Maximum Hadoop split-size; if unset, default to underlying FileSystem's value. Integers as well as byte-size short-hands accepted, e.g. 64m, 32MB
```

#### [`check-bam`][check/Main]

- Run [spark-bam] and [hadoop-bam][] [`Checker`s][`Checker`] over every (uncompressed) position in a BAM file
- Print statistics about their concordance.

See [example test output files][output/check-bam].

##### Example

```bash
$ spark-submit $CLI_JAR check-bam test_bams/src/main/resources/1.bam
…
1608257 uncompressed positions
583K compressed
Compression ratio: 2.69
4917 reads
5 false positives, 0 false negatives

False-positive-site flags histogram:
	5:	tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp

False positives with succeeding read info:
	39374:30965:	1 before D0N7FACXX120305:6:2301:3845:171905 2/2 76b unmapped read (placed at 1:24795617). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	239479:311:	1 before D0N7FACXX120305:7:1206:9262:21623 2/2 76b unmapped read (placed at 1:24973169). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	484396:46507:	1 before D0N7FACXX120305:7:2107:8337:34383 2/2 76b unmapped read (placed at 1:24981330). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	508565:56574:	1 before C0FR5ACXX120302:4:2202:2280:16832 2/2 76b unmapped read (placed at 1:24981398). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	533464:49472:	1 before D0N7FACXX120305:5:1204:3428:52534 2/2 76b unmapped read (placed at 1:24981468). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
```

#### [`full-check`][full/Main]

- Run the [`full`] checker over every (uncompressed) position in a BAM file
- Print statistics about the frequency with which [the record-validity-checks][checks table] used by [spark-bam] correctly ruled out non-record-boundary positions. 

In particular, positions where only one or two checks ruled out a "true negative" can be useful for developing a sense of whether the current battery of checks is sufficient.

[Sample outputs can be found in the tests][output/full-check].

##### Example

```bash
spark-submit $CLI_JAR full-check -l 10 test_bams/src/main/resources/1.bam
…
1608257 uncompressed positions
583K compressed
Compression ratio: 2.69
4917 reads
All calls matched!

No positions where only one check failed

637 positions where exactly two checks failed:
	14146:8327:	296 before D0N7FACXX120305:2:2107:10453:199544 1/2 76b aligned read @ 1:24795498. Failing checks: tooLargeNextReadIdx,invalidCigarOp
	39374:60226:	2 before C0FR5ACXX120302:8:1303:16442:188614 2/2 76b aligned read @ 1:24838240. Failing checks: nonNullTerminatedReadName,invalidCigarOp
	39374:60544:	2 before C0FR5ACXX120302:8:1303:16442:188614 1/2 76b aligned read @ 1:24838417. Failing checks: nonNullTerminatedReadName,invalidCigarOp
	39374:60862:	2 before C0FR5ACXX120302:3:1203:11738:62429 2/2 76b aligned read @ 1:24840504. Failing checks: nonNullTerminatedReadName,invalidCigarOp
	39374:61179:	2 before C0FR5ACXX120302:3:1203:11758:62415 2/2 76b aligned read @ 1:24840504. Failing checks: nonNullTerminatedReadName,invalidCigarOp
	39374:61496:	2 before C0FR5ACXX120302:3:2206:18762:165472 1/2 76b aligned read @ 1:24840609. Failing checks: nonNullTerminatedReadName,invalidCigarOp
	39374:61814:	2 before C0FR5ACXX120302:4:1107:12894:63952 2/2 76b aligned read @ 1:24840639. Failing checks: nonNullTerminatedReadName,invalidCigarOp
	39374:62131:	2 before D0N7FACXX120305:6:1101:10376:28505 2/2 76b aligned read @ 1:24840639. Failing checks: nonNullTerminatedReadName,invalidCigarOp
	39374:62448:	2 before D0N7FACXX120305:3:1102:11046:29695 2/2 76b aligned read @ 1:24840667. Failing checks: nonNullTerminatedReadName,invalidCigarOp
	39374:62765:	2 before C0FR5ACXX120302:2:2301:2419:18406 2/2 76b aligned read @ 1:24840675. Failing checks: nonNullTerminatedReadName,invalidCigarOp
	…

	Histogram:
		626:	nonNullTerminatedReadName,invalidCigarOp
		11:	tooLargeNextReadIdx,invalidCigarOp

	Per-flag totals:
		           invalidCigarOp:	637
		nonNullTerminatedReadName:	626
		      tooLargeNextReadIdx:	 11

Total error counts:
	             invalidCigarOp:	1530730
	            tooLargeReadIdx:	1492210
	        tooLargeNextReadIdx:	1492210
	  nonNullTerminatedReadName:	1320219
	tooFewRemainingBytesImplied:	1218351
	           nonASCIIReadName:	 136637
	                 noReadName:	 124817
	            negativeReadIdx:	  71206
	            negativeReadPos:	  71206
	        negativeNextReadIdx:	  71206
	        negativeNextReadPos:	  71206
	             emptyMappedSeq:	  57182
	            tooLargeReadPos:	  23548
	        tooLargeNextReadPos:	  23548
	              emptyReadName:	  19866
	     tooFewBytesForReadName:	     76
	           emptyMappedCigar:	     26
	     tooFewBytesForCigarOps:	     14
```

#### [`compute-splits`][spark/Main]

Test computation on a given BAM using [spark-bam] and/or [hadoop-bam].

By default, compare them and output any differences.

Some timing information is also output for each, though for spark-bam it is usually dominated by Spark-job-setup overhead that doesn't accurately reflect the time spark-bam spends computing splits

##### Example
Highlighting a hadoop-bam false-positive on a local test BAM:
 
```bash
$ spark-submit $CLI_JAR compute-splits -m 210k test_bams/src/main/resources/1.bam
…
2 splits differ (totals: 3, 3):
		239479:311-430080:65535
	239479:312-435247:181
```

The BAM `1.bam` is a ≈600KB excerpt from the TCGA BAM [64-1681-01A-11D-2063-08.1.bam][1.bam], where hadoop-bam picks an invalid split from offset 64MB.
 
The 105KB split-size used above drops hadoop-bam into the same position in the excerpted BAM, reproducing the bug in a smaller test-case. 

#### [`compare-splits`][compare/Main]

Compare [spark-bam] and [hadoop-bam] splits on multiple/many BAMs.

Similar to [`compute-splits`], but [the {sole,required} path argument][required path arg] points to a file with many BAM paths, one per line. 

Statistics and diffs about spark-bam's and hadoop-bam's computed splits on all of these (or a subset given by `-s`/`-n` flags) are output.

##### Example

[`cli/src/test/resources/test-bams`] contains the 2 test BAM files in this repo, each listed twice:

```bash
$ spark-submit $CLI_JAR compare-splits -m 105k cli/src/test/resources/test-bams
…
2 of 4 BAMs' splits didn't match (totals: 22, 22; 2, 2 unmatched)

Total split-computation time:
	hadoop-bam:	630
	spark-bam:	8359

Ratios:
N: 4, μ/σ: 13.4/1.6, med/mad: 12.8/0.6
sorted: 11.8 12.7 12.9 16.1

	1.bam: 2 splits differ (totals: 6, 6; mismatched: 1, 1):
			239479:311-322560:65535
		239479:312-336825:304
	1.noblocks.bam: 2 splits differ (totals: 6, 6; mismatched: 1, 1):
			239479:311-322560:65535
		239479:312-336825:304
```

- As [above][`compute-splits`], the 105KB split size is chosen to illustrate a hadoop-bam false-positive that's been isolated/reproduced here.
- `1.noblocks.bam` and `1.bam` are identical; the former is a symlink to the latter used for testing in the (apparent) absence of `.bam.blocks` and `.bam.records` files.

#### [`index-records`][IndexRecords]

Outputs a `.bam.records` file with "virtual offsets" of all BAM records in a `.bam` file; see [the test data][test_bams] or [`IndexRecordsTest`] for example output:

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

This runs in one thread on one node and doesn't use Spark, which can take a long time, but is the only/best way to be certain of BAM-record-boundaries.

[`check-bam`]'s default mode doesn't consult a `.records` file, but rather compares spark-bam's and hadoop-bam's read-position calls; as long as they're not both incorrect at the same position, that is an easier way to evaluate them (there are no known positions where spark-bam is incorrect).

##### Example usage

```bash
spark-submit $CLI_JAR index-records <input.bam>
```

#### [`index-blocks`][IndexBlocks]

Outputs a `.bam.blocks` file with {start position, compressed size, and uncompressed size} for each BGZF block in an input `.bam` file; see [the test data][test_bams] or [`IndexBlocksTest`] for example output:

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

No commands require running `index-blocks`, as Spark-parallelized BGZF-splitting is also implemented in this repo and can be used as a fallback in the absence of a `.blocks` file.

BGZF-block-splitting is a much more straightforward task than BAM-record-boundary-splitting, and is not believed to be a source of incorrectness in spark-bam or hadoop-bam, but this command can be used to be extra careful, if desired.

##### Example usage

```bash
spark-submit $CLI_JAR index-blocks <input.bam>
```

#### [`htsjdk-rewrite`][rewrite/Main]

- Round-trips a BAM file through HTSJDK, which writes it out without aligning BAM records to BGZF-block starts
- Useful for creating BAMs with which to test [hadoop-bam]'s correctness
	- Some tools (e.g. `samtools`) align reads to BGZF-block boundaries
	- [spark-bam] and [hadoop-bam] are both always correct in such cases

##### Example

This command was used to generate the test file [`cli/src/test/resources/slice/2.100-1000.bam`][cli/str/slice], which contains the reads from [`test_bams/src/main/resources/2.bam`][test_bams] with indices in the range [100, 1000):

```bash
spark-submit $CLI_JAR \
	htsjdk-rewrite \
	-r 100-3000 \
	test_bams/src/main/resources/2.bam \
	cli/src/test/resources/slice/2.100-1000.bam
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
[`eager`]: #eager
[`seqdoop`]: #seqdoop
[`full`]: #full
[`indexed`]: #indexed
[api-clarity]: #algorithm-api-clarity

<!-- Checkers -->
[eager/Checker]: check/src/main/scala/org/hammerlab/bam/check/eager/Checker.scala
[full/Checker]: check/src/main/scala/org/hammerlab/bam/check/full/Checker.scala
[seqdoop/Checker]: seqdoop/src/main/scala/org/hammerlab/bam/check/seqdoop/Checker.scala
[indexed/Checker]: check/src/main/scala/org/hammerlab/bam/check/indexed/Checker.scala

[`Checker`]: src/main/scala/org/hammerlab/bam/check/Checker.scala

<!-- test/resources links -->
[`cli/src/test/resources/test-bams`]: cli/src/test/resources/test-bams
[output/check-bam]: cli/src/test/resources/output/check-bam
[output/full-check]: cli/src/test/resources/output/full-check

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
[Main]: cli/src/main/scala/org/hammerlab/bam/Main.scala

[`check-bam`]: #check
[check/Main]: cli/src/main/scala/org/hammerlab/bam/check/Main.scala

[`full-check`]: #full-check
[full/Main]: cli/src/main/scala/org/hammerlab/bam/check/full/Main.scala

[`compute-splits`]: #compute-splits
[spark/Main]: cli/src/main/scala/org/hammerlab/bam/spark/Main.scala

[`compare-splits`]: #compare-splits
[compare/Main]: cli/src/main/scala/org/hammerlab/bam/compare/Main.scala

[`index-blocks`]: #index-blocks
[IndexBlocks]: cli/src/main/scala/org/hammerlab/bgzf/index/IndexBlocks.scala
[`IndexBlocksTest`]: cli/src/test/scala/org/hammerlab/bgzf/index/IndexBlocksTest.scala

[`index-records`]: #index-records
[IndexRecords]: cli/src/main/scala/org/hammerlab/bam/index/IndexRecords.scala
[`IndexRecordsTest`]: cli/src/test/scala/org/hammerlab/bam/index/IndexRecordsTest.scala

[`htsjdk-rewrite`]: #htsjdk-rewrite
[rewrite/Main]: cli/src/main/scala/org/hammerlab/bam/rewrite/Main.scala

<!-- External BAM links -->
[1.bam]: https://portal.gdc.cancer.gov/legacy-archive/files/19155553-8199-4c4d-a35d-9a2f94dd2e7d
[2.bam]: https://portal.gdc.cancer.gov/legacy-archive/files/b7ee4c39-1185-4301-a160-669dea90e192

[`org.hammerlab.paths.Path`]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala
[Path NIO ctor]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala#L14
[Path URI ctor]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala#L157
[Path String ctor]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala#L145-L155

[`SAMRecord`]: https://github.com/samtools/htsjdk/blob/2.9.1/src/main/java/htsjdk/samtools/SAMRecord.java

[`CanLoadBam`]: load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala
[`loadReads`]: load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#TODO
[`loadBamIntervals`]: load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#TODO
[`loadReadsAndPositions`]: load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#TODO
[`loadSplitsAndReads`]: load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#TODO

[`LociSet`]: https://github.com/hammerlab/genomic-loci/blob/2.0.1/src/main/scala/org/hammerlab/genomics/loci/set/LociSet.scala

[`Pos`]: bgzf/src/main/scala/org/hammerlab/bgzf/Pos.scala
[`Split`]: check/src/main/scala/org/hammerlab/bam/spark/Split.scala

[linking]: #linking-against-spark-bam

[test_bams]: test_bams/src/main/resources
[cli/str/slice]: cli/src/test/resources/slice

[cli]: cli
