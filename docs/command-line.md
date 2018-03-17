# Command-line interface

[spark-bam's cli module][cli] includes several standalone apps as subcommands:

- **Spark apps**
	- [`check-bam`]
	- [`check-blocks`]
	- [`full-check`]
	- [`compute-splits`]
	- [`compare-splits`]
	- [`count-reads`]
	- [`time-load`]

- **Single-node / Non-Spark apps**
	- [`index-blocks`]
	- [`index-records`]
	- [`htsjdk-rewrite`]

The latter are typically most easily run via `spark-submit` anyway, as in examples below.

## Setup

### Via Maven

```bash
wget -O spark-bam-cli.jar https://oss.sonatype.org/content/repositories/releases/org/hammerlab/bam/cli_2.11/1.2.0-M1/cli_2.11-1.2.0-M1-assembly.jar
export CLI_JAR=spark-bam-cli.jar
```

### From source

```bash
sbt cli/assembly
export CLI_JAR=cli/target/scala-2.11/cli-assembly-1.2.0-M1.jar
```

## Running a subcommand

```bash
spark-submit <spark confs> $CLI_JAR <subcommand> <options>
```

### Example
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

### Help / Usage
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

### Common arguments / options

#### Required argument: `<path>`
Most commands require exactly one argument: a path to operate on in some way.

This is usually a BAM file, but in the case of [`compare-splits`] is a file with lists of BAM-file paths.

#### Output path
They also take an optional "output path" via `--output-path` or `-o`, which they write their meaningful output to; if not set, stdout is used.

```
--output-path | -o  <path>
      Print output to this file, otherwise to stdout
```

#### Print limit
Commands' output is also governed by the "print-limit" (`--print-limit` / `-l`) option.

Various portions of output data will be capped to this number of records/lines to avoid overloading the Spark driver with large `collect` jobs in cases where a prodigious amount of records match some criteria (e.g. false-positive positions in a [`check-bam`] run).

```
--print-limit | -l  <num=1000000>
      When collecting samples of records/results for displaying to the user, limit to this many to the user, limit to this many to avoid overloading the driver
```

#### `--split-size` / `-m`
Many commands are sensitive to the split-size. Default is often taken from the underlying Hadoop FileSystem, though in e.g. `check-bam` it is set to 2MB to keep the work per Spark-task manageable.

```
--split-size | --max-split-size | -m  <bytes>
      Maximum Hadoop split-size; if unset, default to underlying FileSystem's value. Integers as well as byte-size short-hands accepted, e.g. 64m, 32MB
```

## [check-bam][check/Main]

- Run [spark-bam] and [hadoop-bam][] [`Checker`s][`Checker`] over every (uncompressed) position in a BAM file
- Print statistics about their concordance.

See [example test output files][output/check-bam].

### Example

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

## [check-blocks][blocks/Main]

- Run [spark-bam] and [hadoop-bam][] [`Checker`s][`Checker`] from the start of every BGZF block in a BAM file
- Print statistics about their concordance
- This is a good high-level estimate for the frequency and size of BAM-file portions that [hadoop-bam] will get wrong 

See [tests for example output][blocks/MainTest].


## [full-check][full/Main]

- Run the [`full`] checker over every (uncompressed) position in a BAM file
- Print statistics about the frequency with which [the record-validity-checks][checks table] used by [spark-bam] correctly ruled out non-record-boundary positions. 

In particular, positions where only one or two checks ruled out a "true negative" can be useful for developing a sense of whether the current battery of checks is sufficient.

[Sample outputs can be found in the tests][output/full-check].

### Example

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

## [compute-splits][spark/Main]

Test computation on a given BAM using [spark-bam] and/or [hadoop-bam].

By default, compare them and output any differences.

Some timing information is also output for each, though for spark-bam it is usually dominated by Spark-job-setup overhead that doesn't accurately reflect the time spark-bam spends computing splits

### Example
Highlighting a hadoop-bam false-positive on a local test BAM:
 
```bash
$ spark-submit $CLI_JAR compute-splits -m 210k test_bams/src/main/resources/1.bam
…
2 splits differ (totals: 3, 3):
		239479:311-430080:65535
	239479:312-435247:181
```

The BAM `1.bam` is a ≈600KB excerpt from the TCGA BAM [19155553-8199-4c4d-a35d-9a2f94dd2e7d], where hadoop-bam picks an invalid split from offset 64MB.
 
The 105KB split-size used above drops hadoop-bam into the same position in the excerpted BAM, reproducing the bug in a smaller test-case. 

## [compare-splits][compare/Main]

Compare [spark-bam] and [hadoop-bam] splits on multiple/many BAMs.

Similar to [`compute-splits`], but [the {sole,required} path argument][required path arg] points to a file with many BAM paths, one per line. 

Statistics and diffs about spark-bam's and hadoop-bam's computed splits on all of these (or a subset given by `-s`/`-n` flags) are output.

### Example

[`test-bams`][`cli/src/test/resources/test-bams`] contains the 2 test BAM files in this repo, each listed twice:

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
- The "Total split-computation time" and "Ratios" sections show that [spark-bam] was ≈13x slower than [hadoop-bam] in this run.
 	- In larger runs, differences more like ≈5x are typical.
 	- Relative slowness likely results from emphasis on abstraction clarity and no profiling having been done.
 	- It's not considered particularly problematic: the expectation is that parallelization more than compensates for it (e.g. a 100-core cluster would be 20x fasterwith 5x the total CPU use).

## [count-reads][CountReads]
- Count the reads in a BAM with [spark-bam] and [hadoop-bam]
- Output the time taken by each as well as whether the counts matched

### Example

```bash
spark-submit $CLI_JAR count-reads -m 100k test_bams/src/main/resources/1.bam
…
spark-bam read-count time: 1361
hadoop-bam read-count time: 1784

Read counts matched: 4917
```

These numbers are not very meaningful on small BAMs / local mode; Spark-setup overhead tends to differentially count against whichever side is run first ([hadoop-bam] by default, [spark-bam] when the `-s` flag is provided):

```bash
spark-submit $CLI_JAR count-reads -m 100k -s test_bams/src/main/resources/1.bam
…
spark-bam read-count time: 3670
hadoop-bam read-count time: 1184

Read counts matched: 4917
```

## [time-load][TimeLoad]
- Collect the first read from every partition to the driver with each [spark-bam] and [hadoop-bam]
- Output the time taken by each as well as any differences in the collected reads.

### Example

```bash
spark-submit $CLI_JAR time-load -m 100k test_bams/src/main/resources/1.bam
…
spark-bam first-read collection time: 1163
hadoop-bam first-read collection time: 2019

All 6 partition-start reads matched
```

As above, larger/cluster runs will give more interesting values here, and the `-s` flag will run [spark-bam] first.

## [index-records][IndexRecords]

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

### Example usage

```bash
spark-submit $CLI_JAR index-records <input.bam>
```

## [index-blocks][IndexBlocks]

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

### Example usage

```bash
spark-submit $CLI_JAR index-blocks <input.bam>
```

## [htsjdk-rewrite][rewrite/Main]

- Round-trips a BAM file through HTSJDK, which writes it out without aligning BAM records to BGZF-block starts
- Useful for creating BAMs with which to test [hadoop-bam]'s correctness
	- Some tools (e.g. `samtools`) align reads to BGZF-block boundaries
	- [spark-bam] and [hadoop-bam] are both always correct in such cases

### Example

This command was used to generate the test file [`2.100-1000.bam`][cli/str/slice], which contains the reads from [`2.bam`][test_bams] with indices in the range [100, 1000):

```bash
spark-submit $CLI_JAR \
	htsjdk-rewrite \
	-r 100-3000 \
	test_bams/src/main/resources/2.bam \
	cli/src/test/resources/slice/2.100-1000.bam
```


<!-- Command/Subcommand links -->
[Main]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/Main.scala

[`check-bam`]: #check-bam
[check/Main]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/check/Main.scala

[`check-blocks`]: #check-blocks
[blocks/Main]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/check/blocks/Main.scala
[blocks/MainTest]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/scala/org/hammerlab/bam/check/blocks/MainTest.scala

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

[`count-reads`]: #count-reads
[CountReads]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/spark/compare/CountReads.scala

[`time-load`]: #time-load
[TimeLoad]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/spark/compare/TimeLoad.scala

[`htsjdk-rewrite`]: #htsjdk-rewrite
[rewrite/Main]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/rewrite/Main.scala

[test_bams]: test_bams/src/main/resources
[cli/str/slice]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/resources/slice

[cli]: https://github.com/hammerlab/spark-bam/blob/master/cli

[`Checker`]: src/main/scala/org/hammerlab/bam/check/Checker.scala

[hadoop-bam]: https://github.com/HadoopGenomics/Hadoop-BAM
[spark-bam]: https://github.com/hammerlab/spark-bam

<!-- External BAM links -->
[19155553-8199-4c4d-a35d-9a2f94dd2e7d]: https://portal.gdc.cancer.gov/legacy-archive/files/19155553-8199-4c4d-a35d-9a2f94dd2e7d
[b7ee4c39-1185-4301-a160-669dea90e192]: https://portal.gdc.cancer.gov/legacy-archive/files/b7ee4c39-1185-4301-a160-669dea90e192

[required path arg]: #required-argument-path

<!-- Checker links -->
[`eager`]: #eager
[`seqdoop`]: #seqdoop
[`full`]: #full
[`indexed`]: #indexed

[checks table]: #improved-record-boundary-detection-robustness

<!-- test/resources links -->
[`cli/src/test/resources/test-bams`]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/resources/test-bams
[output/check-bam]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/resources/output/check-bam
[output/full-check]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/resources/output/full-check
