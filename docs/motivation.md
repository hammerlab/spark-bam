# Motivation

[spark-bam] improves on [hadoop-bam] in 3 ways:

- [parallelization][parallelization-section]
- [correctness][correctness-section]
- [algorithm/API clarity][algorithm-clarity-section]

## Parallelization

[hadoop-bam] computes splits sequentially on one node. Depending on the storage backend, this can take many minutes for modest-sized (10-100GB) BAMs, leaving a large cluster idling while the driver bottlenecks on an emminently-parallelizable task.
 
For example, on Google Cloud Storage (GCS), two factors causing high split-computation latency include:

- high GCS round-trip latency
- file-seek/-access patterns that nullify buffering in GCS NIO/HDFS adapters

[spark-bam] identifies record-boundaries in each underlying file-split in the same Spark job that streams through the records, eliminating the driver-only bottleneck and maximally parallelizing split-computation.

## Correctness

An important impetus for the creation of [spark-bam] was the discovery of two TCGA lung-cancer BAMs for which [hadoop-bam] produces invalid splits:

- [19155553-8199-4c4d-a35d-9a2f94dd2e7d]
- [b7ee4c39-1185-4301-a160-669dea90e192]

HTSJDK threw an error when trying to parse reads from essentially random data fed to it by [hadoop-bam]:

```
MRNM should not be set for unpaired read
```

These BAMs were rendered unusable, and questions remain around whether such invalid splits could silently corrupt analyses.
 
### Improved record-boundary-detection robustness

[spark-bam] fixes these record-boundary-detection "false-positives" by adding additional checks:
  
| Validation check | spark-bam | hadoop-bam |
| --- | --- | --- |
| Negative reference-contig idx | ✅ | ✅ |
| Reference-contig idx too large | ✅ | ✅ |
| Negative locus | ✅ | ✅ |
| Locus too large | ✅ | 🚫 |
| Read-name ends with `\0` | ✅ | ✅ |
| Read-name non-empty | ✅ | 🚫 |
| Invalid read-name chars | ✅ | 🚫 |
| Record length consistent w/ #{bases, cigar ops} | ✅ | ✅ |
| Cigar ops valid | ✅ | 🌓* |
| Subsequent reads valid | ✅ | ✅ |
| Non-empty cigar/seq in mapped reads | ✅ | 🚫 |
| Cigar consistent w/ seq len | 🚫 | 🚫 |


\* Cigar-op validity is not verified for the "record" that anchors a record-boundary candidate BAM position, but it is verified for the *subsequent* records that hadoop-bam checks

### Checking correctness

[spark-bam] detects BAM-record boundaries using the pluggable [`Checker`] interface.

Four implementations are provided:

#### [eager][eager/Checker]

Default/Production-worthy record-boundary-detection algorithm:

- includes [all the checks listed above][checks table]
- rules out a position as soon as any check fails
- can be compared against [hadoop-bam]'s checking logic (represented by the [`seqdoop`] checker) using the [`check-bam`], [`compute-splits`], and [`compare-splits`] commands
- used in BAM-loading APIs exposed to downstream libraries

#### [full][full/Checker]

Debugging-oriented [`Checker`]:

- runs [all the checks listed above][checks table]
- emits information on all checks that passed or failed at each position
    - useful for downstream analysis of the accuracy of individual checks or subsets of checks
    - see [the `full-check` command][`full-check`] or its stand-alone "main" app at [`org.hammerlab.bam.check.full.Main`][full/Main]
    - see [sample output in tests](src/test/scala/org/hammerlab/bam/check/full/MainTest.scala#L276-L328)

#### [seqdoop][seqdoop/Checker]

[`Checker`] that mimicks [hadoop-bam]'s [`BAMSplitGuesser`] as closely as possible.

- Useful for analyzing [hadoop-bam]'s correctness
- Uses the [hammerlab/hadoop-bam] fork, which exposes [`BAMSplitGuesser`] logic more efficiently/directly

#### [indexed][indexed/Checker]

This [`Checker`] simply reads from a `.records` file (as output by [`index-records`]) and reflects the read-positions listed there.
 
It can serve as a "ground truth" against which to check either the [`eager`] or [`seqdoop`] checkers (using the `-s` or `-u` flags to [`check-bam`], resp.).

### Future-proofing

[hadoop-bam] is poorly suited to handling increasingly-long reads from e.g. PacBio and Oxford Nanopore sequencers.
 
For example, a 100kbp-long read is likely to span multiple BGZF blocks, causing [hadoop-bam] to reject it as invalid.

[spark-bam] is robust to such situations, related to [its agnosticity about buffer-sizes / reads' relative positions with respect to BGZF-block boundaries][algorithm-clarity-section].

## Algorithm/API clarity

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
  
### Case study: counting on OOMs

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
		- the position is not actually valid!  💥 🚫 😱
	- if the allocation fails, [an OOM is caught and taken to signal that this is not a valid record position](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java#L212) (which is true!)

This resulted in positions that hadoop-bam correctly ruled out in sufficiently-memory-constrained test-contexts, but false-positived on in more-generously-provisioned settings, which is obviously an undesirable relationship to correctness.


<!-- Repos -->
[hadoop-bam]: https://github.com/HadoopGenomics/Hadoop-BAM
[spark-bam]: https://github.com/hammerlab/spark-bam
[hammerlab/hadoop-bam]: https://github.com/hammerlab/Hadoop-BAM/tree/7.9.0

<!-- Sections -->
[parallelization-section]: #parallelization
[correctness-section]: #correctness
[algorithm-clarity-section]: #algorithmapi-clarity

<!-- External BAM links -->
[19155553-8199-4c4d-a35d-9a2f94dd2e7d]: https://portal.gdc.cancer.gov/legacy-archive/files/19155553-8199-4c4d-a35d-9a2f94dd2e7d
[b7ee4c39-1185-4301-a160-669dea90e192]: https://portal.gdc.cancer.gov/legacy-archive/files/b7ee4c39-1185-4301-a160-669dea90e192

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

[`index-records`]: #index-records
[IndexRecords]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/main/scala/org/hammerlab/bam/index/IndexRecords.scala
[`IndexRecordsTest`]: https://github.com/hammerlab/spark-bam/blob/master/cli/src/test/scala/org/hammerlab/bam/index/IndexRecordsTest.scala


[`BAMSplitGuesser`]: https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMSplitGuesser.java

[checks table]: #improved-record-boundary-detection-robustness
