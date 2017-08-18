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
    - see [the `full-check` command][`full-check`] or its stand-alone "main" app at [`org.hammerlab.bam.check.full.Main`][full/Main]
    - see [sample output in tests](src/test/scala/org/hammerlab/bam/check/full/MainTest.scala#L276-L328)

##### [`seqdoop`][]

[`Checker`][] that mimicks [hadoop-bam][]'s [`BAMSplitGuesser`][] as closely as possible.

- Useful for analyzing [hadoop-bam][]'s correctness
- Uses the [hammerlab/hadoop-bam][] fork, which exposes [`BAMSplitGuesser`][] logic more efficiently/directly

##### [`indexed`][]

This [`Checker`][] simply reads from a `.records` file (as output by [`index-records`][]) and reflects the read-positions listed there.
 
It can serve as a "ground truth" against which to check either the [`eager`][eager] or [`seqdoop`][seqdoop] checkers (using the `-s` or `-u` flags to [`check-bam`][], resp.).

#### Future-proofing

Some assumptions in [hadoop-bam][] are likely to break when processing long reads.
 
For example, a 100kbp-long read is likely to span multiple BGZF blocks, likely causing [hadoop-bam][] to reject it as valid.

It is believed that [spark-bam][] will be robust to such situations, related to [its agnosticity about buffer-sizes / reads' relative positions with respect to BGZF-block boundaries][api-clarity], though this has not been tested.


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
$ spark-submit $SPARK_BAM_JAR compare-splits -h
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

This is usually a BAM file, but in the case of [`compare-splits`][] is a file with lists of BAM-file paths.

###### Output path
They also take an optional "output path" via `--output-path` or `-o`, which they write their meaningful output to; if not set, stdout is used.

```
--output-path | -o  <path>
      Print output to this file, otherwise to stdout
```

###### Print limit
Commands' output is also governed by the "print-limit" (`--print-limit` / `-l`) option.

Various portions of output data will be capped to this number of records/lines to avoid overloading the Spark driver with large `collect` jobs in cases where a prodigious amount of records match some criteria (e.g. false-positive positions in a [`check-bam`][] run).

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

- Run [spark-bam][] and [hadoop-bam][] [`Checker`s][`Checker`] over every (uncompressed) position in a BAM file
- Print statistics about their concordance.

See [example test output files][output/check-bam].

##### Example

```bash
$ spark-submit $SPARK_BAM_JAR check-bam src/test/resources/1.2203053-2211029.bam
…
2580596 uncompressed positions
941K compressed
Compression ratio: 2.68
7976 reads
9 false positives, 0 false negatives

False-positive-site flags histogram:
	9:	tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp

False positives with succeeding read info:
	39374:30965:	1 before D0N7FACXX120305:6:2301:3845:171905 2/2 76b unmapped read (placed at 1:24795617). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	366151:51533:	1 before C0FR5ACXX120302:4:1204:6790:58160 2/2 76b unmapped read (placed at 1:24932215). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	391261:35390:	1 before C0FR5ACXX120302:4:1106:5132:48894 2/2 76b unmapped read (placed at 1:24969786). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	463275:65228:	1 before D0N7FACXX120305:4:1102:8753:123279 2/2 76b unmapped read (placed at 1:24973169). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	486847:6:	1 before D0N7FACXX120305:7:1206:9262:21623 2/2 76b unmapped read (placed at 1:24973169). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	731617:46202:	1 before D0N7FACXX120305:7:2107:8337:34383 2/2 76b unmapped read (placed at 1:24981330). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	755781:56269:	1 before C0FR5ACXX120302:4:2202:2280:16832 2/2 76b unmapped read (placed at 1:24981398). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	780685:49167:	1 before D0N7FACXX120305:5:1204:3428:52534 2/2 76b unmapped read (placed at 1:24981468). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
	855668:64691:	1 before D0N7FACXX120305:5:1308:8464:128307 1/2 76b unmapped read (placed at 1:24987247). Failing checks: tooLargeReadPos,tooLargeNextReadPos,emptyReadName,invalidCigarOp
```

#### [`full-check`][full/Main]

- Run the [`full`][] checker over every (uncompressed) position in a BAM file
- Print statistics about the frequency with which [the record-validity-checks][checks table] used by [spark-bam][] correctly ruled out non-record-boundary positions. 

In particular, positions where only one or two checks ruled out a "true negative" can be useful for developing a sense of whether the current battery of checks is sufficient.

[Sample outputs can be found in the tests][output/full-check].

##### Example

```bash
spark-submit $SPARK_BAM_JAR full-check src/test/resources/1.2203053-2211029.bam
…
2580596 uncompressed positions
941K compressed
Compression ratio: 2.68
7976 reads
All calls matched!

No positions where only one check failed

10 of 1208 positions where exactly two checks failed:
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
		1193:	nonNullTerminatedReadName,invalidCigarOp
		15:	tooLargeNextReadIdx,invalidCigarOp

	Per-flag totals:
	             invalidCigarOp:	1208
	  nonNullTerminatedReadName:	1193
	        tooLargeNextReadIdx:	  15

Total error counts:
             invalidCigarOp:	2454770
        tooLargeNextReadIdx:	2391574
            tooLargeReadIdx:	2391574
  nonNullTerminatedReadName:	2113790
tooFewRemainingBytesImplied:	1965625
           nonASCIIReadName:	 221393
                 noReadName:	 202320
        negativeNextReadIdx:	 116369
            negativeReadIdx:	 116369
            negativeReadPos:	 116369
        negativeNextReadPos:	 116369
        tooLargeNextReadPos:	  37496
            tooLargeReadPos:	  37496
              emptyReadName:	  32276
     tooFewBytesForReadName:	     77
     tooFewBytesForCigarOps:	      9
```

#### [`compute-splits`][spark/Main]

Test and time split computation on a given BAM using [spark-bam][] and/or [hadoop-bam][].

##### Example
Highlighting a hadoop-bam false-positive on a local test BAM:
 
```bash
$ spark-submit $SPARK_BAM_JAR compute-splits -m 470k src/test/resources/1.2203053-2211029.bam
…
2 splits differ (totals: 2, 2):
		486847:6-963864:65535
	486847:7-963864:0
```

The BAM `1.2203053-2211029.bam` is a ≈1MB excerpt from the TCGA BAM [64-1681-01A-11D-2063-08.1.bam][1.bam], where hadoop-bam picks an invalid split from offset 64MB.
 
The 470KB split-size used above drops hadoop-bam into the same position in the excerpted BAM, reproducing the bug in a smaller test-case. 

#### [`compare-splits`][compare/Main]

Compare [spark-bam][] and [hadoop-bam][] splits on multiple/many BAMs.

Similar to [`compute-splits`][], but [the {sole,required} path argument][required path arg] points to a file with many BAM paths, one per line. 

Statistics and diffs about spark-bam's and hadoop-bam's computed splits on all of these (or a subset given by `-s`/`-n` flags) are output.

##### Example

[`src/test/resources/test-bams`][] contains the 4 test BAM files in this repo:

```bash
$ spark-submit $SPARK_BAM_JAR compare-splits -m 470k src/test/resources/test-bams
…
2 of 4 BAMs' splits didn't match (totals: 9, 9; 2, 2 unmatched):

	1.2203053-2211029.bam: 2 splits differ (totals: 2, 2; mismatched: 1, 1):
				Split(486847:6,963864:65535)
			Split(486847:7,963864:0)
	1.2203053-2211029.noblocks.bam: 2 splits differ (totals: 2, 2; mismatched: 1, 1):
				Split(486847:6,963864:65535)
			Split(486847:7,963864:0)
```

- As [above][`compute-splits`], the 470KB split size is chosen to illustrate a hadoop-bam false-positive that's been isolated/reproduced here.
- `1.2203053-2211029.noblocks.bam` and `1.2203053-2211029.bam` are identical; the former is a symlink to the latter used for testing in the (apparent) absence of `.bam.blocks` and `.bam.records` files.

#### [`index-records`][IndexRecords]

Outputs a `.bam.records` file with "virtual offsets" of all BAM records in a `.bam` file; see [the test data][str] or [`IndexRecordsTest`][] for example output:

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

[`check-bam`][]'s default mode doesn't consult a `.records` file, but rather compares spark-bam's and hadoop-bam's read-position calls; as long as they're not both incorrect at the same position, that is an easier way to evaluate them (there are no known positions where spark-bam is incorrect).

##### Example usage

```bash
spark-submit $SPARK_BAM_JAR index-records <input.bam>
```

#### [`index-blocks`][IndexBlocks]

Outputs a `.bam.blocks` file with {start position, compressed size, and uncompressed size} for each BGZF block in an input `.bam` file; see [the test data][str] or [`IndexBlocksTest`][] for example output:

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
spark-submit $SPARK_BAM_JAR index-blocks <input.bam>
```

#### [`htsjdk-rewrite`][rewrite/Main]

- Round-trips a BAM file through HTSJDK, which writes it out without aligning BAM records to BGZF-block starts
- Useful for creating BAMs with which to test [hadoop-bam][]'s correctness
	- Some tools (e.g. `samtools`) align reads to BGZF-block boundaries
	- [spark-bam][] and [hadoop-bam][] are both always correct in such cases

##### Example

This command was used to generate the test file [`src/test/resources/5k.100-3000/5k.100-3000.bam`][str/5k.100-3000], which contains the reads from [`src/test/resources/5k.bam`][str] with indices in the range [100, 3000):

```bash
spark-submit $SPARK_BAM_JAR \
	htsjdk-rewrite \
	-s 100 -e 3000 \
	src/test/resources/5k.bam \
	src/test/resources/5k.100-3000/5k.100-3000.bam
```


### As a library

#### Depend on [spark-bam][] using Maven

```xml
<dependency>
	<groupId>org.hammerlab</groupId>
	<artifactId>spark-bam_2.11</artifactId>
	<version>1.1.0-SNAPSHOT</version>
</dependency>
```

#### Depend on [spark-bam][] using SBT

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

## Benchmarks

### Accuracy

#### [spark-bam][]

There are no known situations where [spark-bam][] returns an invalid split-start.

#### [hadoop-bam][]

[hadoop-bam][] seems to have a false-positive rate of about 1 in every TODO uncompressed BAM positions.
 
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


<!-- Intra-page links -->
[checks table]: #improved-record-boundary-detection-robustness
[getting an assembly JAR]: #get-an-assembly-JAR
[required path arg]: #required-argument-path
[eager]: #eager
[seqdoop]: #seqdoop
[full]: #full
[indexed]: #indexed
[api-clarity]: #algorithm-api-clarity

<!-- Checkers -->
[`eager`]: src/main/scala/org/hammerlab/bam/check/eager/Checker.scala
[`full`]: src/main/scala/org/hammerlab/bam/check/full/Checker.scala
[`seqdoop`]: src/main/scala/org/hammerlab/bam/check/seqdoop/Checker.scala
[`indexed`]: src/main/scala/org/hammerlab/bam/check/indexed/Checker.scala

[`Checker`]: src/main/scala/org/hammerlab/bam/check/Checker.scala

<!-- test/resources links -->
[str]: src/test/resources
[`src/test/resources/test-bams`]: src/test/resources/test-bams
[output/check-bam]: src/test/resources/output/check-bam
[output/full-check]: src/test/resources/output/full-check

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

<!-- External BAM links -->
[1.bam]: https://portal.gdc.cancer.gov/cases/c583fdd1-8cd2-4c15-a23e-0644261f65da?bioId=3813c301-9622-4567-bc72-d17acbeb236f
[2.bam]: https://portal.gdc.cancer.gov/cases/bcb6447f-3e4c-44fe-afc3-100d5dbe9ba2?bioId=f62f6ba1-59a7-4b16-b69f-15fa3dabfbc1

[str/5k.100-3000]: src/test/resources/5k.100-3000
