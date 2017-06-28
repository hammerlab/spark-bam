# spark-bam
[![Build Status](https://travis-ci.org/hammerlab/spark-bam.svg?branch=master)](https://travis-ci.org/hammerlab/spark-bam)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/spark-bam/badge.svg?branch=master)](https://coveralls.io/github/hammerlab/spark-bam?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/org.hammerlab/spark-bam_2.11.svg?maxAge=600)](http://search.maven.org/#search%7Cga%7C1%7Cspark-bam)

Load [BAM files](http://samtools.github.io/hts-specs/SAMv1.pdf) using [Apache Spark](https://spark.apache.org/) and [HTSJDK](https://github.com/samtools/htsjdk):

```scala
import org.hammerlab.spark.bam._
import import org.apache.hadoop.fs.Path

val reads = sc.loadBam(new Path("my.bam"))  // Default: use [4x the available cores] threads on the driver to compute splits
reads.count
```

Inspired by [HadoopGenomics/hadoop-bam][hadoop-bam].

## Features

[spark-bam][] improves on [hadoop-bam][] in 3 ways:

- [parallelization](#Parallelization)
- [correctness](#Correctness)
- [algorithm/API clarity](#algorithm-api-clarity)

### Parallelization

[spark-bam][] offers two mechanisms for parallelizing BAM-split-computation:

- [using threads on the driver](#TODO)

	```scala
	sc.loadBam(Threads(32))
	```

- [using a Spark job](#TODO)

	```scala
	// Default: 1 element (split offset) per partition
	sc.loadBam(Spark())  

	import org.hammerlab.parallel.spark._

	// Optionally specify a number of splits to compute in each Spark task…
	sc.loadBam(Spark(ElemsPerPartition(10)))

	// …or a total number of tasks ("partitions") to use
	sc.loadBam(Spark(NumPartitions(100)))
	```

#### Motivation

With hadoop-bam's [BAMInputFormat](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMInputFormat.java), computing splits on a Google Cloud Storage (GCS)-resident BAM typically took O(minutes), e.g. [2mins for a 20GB BAM and 15mins on a 178GB BAM in recent benchmarks](#TODO).

During this time, the driver node fetches data from each split-offset returned by `FileInputFormat`, typically ≈4 64KB BGZF blocks (or 256KB) every 32MB, 64MB, or 128MB according to the HDFS-block-size being used (or simulated in the case of a [`GoogleHadoopFS`](https://github.com/GoogleCloudPlatform/bigdata-interop/blob/v1.6.1/gcs/src/main/java/com/google/cloud/hadoop/fs/gcs/GoogleHadoopFS.java)). 

Each network request and resulting split-computation happens in serial, and the network requests exhibit a high, fixed latency-cost.
 
#### Parallelization Strategy #1: driver threads
 
![Depcition of a driver machine analyzing split starts in sequence vs. in parallel](https://cl.ly/3E3p1c2J2q2W/Screen%20Shot%202017-06-28%20at%2011.09.55%20AM.png)

Using N threads to parallelize fetching the start of each `FileInputFormat`-split [exhibits near-perfect linear scaling (up to 64 threads on a 4-core GCE `n1-standard-4` VM)](#TODO)), bringing split-computation from 15mins to 20s on a 178GB BAM and from 2mins to 4s on a 22GB BAM:

TODO: scaling figure

#### Parallelization Strategy #2: Spark job

For especially largs BAMs, parallelizing across many machines can be worthwhile: 

![Depiction of using a Spark job to compute split starts](https://cl.ly/2k0B3p3o3H3L/Screen%20Shot%202017-06-28%20at%2011.15.03%20AM.png)

##### Using:

By default, process one split per Spark partition:

```scala
sc.loadBam(Spark())  
```

Optionally: specify a number of splits to compute in each Spark task:

```scala
import org.hammerlab.parallel.spark._
sc.loadBam(Spark(ElemsPerPartition(10)))
```

…or a total number of tasks ("partitions") to use:
```scala
sc.loadBam(Spark(NumPartitions(100)))
```

### Correctness
An important impetus for the creation of [spark-bam][] was the discovery of two TCGA lung-cancer BAMs for which [hadoop-bam][] produces invalid splits: [64-1681-01A-11D-2063-08.1.bam](https://portal.gdc.cancer.gov/cases/c583fdd1-8cd2-4c15-a23e-0644261f65da?bioId=3813c301-9622-4567-bc72-d17acbeb236f) and [85-A512-01A-11D-A26M-08.6.bam](https://portal.gdc.cancer.gov/cases/bcb6447f-3e4c-44fe-afc3-100d5dbe9ba2?bioId=f62f6ba1-59a7-4b16-b69f-15fa3dabfbc1).

HTSJDK threw an error downstream in these cases:

```
MRNM should not be set for unpaired read
```

but the BAMs were rendered unusable, and questions remained around whether such invalid splits could ever silently corrupt analyses.
 
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
 
#### `Checker` interface

[spark-bam][] formalizes

### Algorithm/API clarity

## Using

### Hello World

### Via Maven/SBT
You should be able to depend on the most recent release, [`org.hammerlab:hadoop_bam:1.0.0`](https://oss.sonatype.org/content/repositories/releases/org/hammerlab/hadoop-bam_2.11/1.0.0/), and use `org.hammerlab.hadoop_bam.BAMInputFormat` where you'd otherwise have used `org.seqdoop.hadoop_bam.BAMInputFormat`.

In particular, using `org.seqdoop.hadoop_bam.BAMInputFormat` from Java code should work fine, as it is a vanilla Java class, but this has not been tested.

### Running Locally
To build/run locally, try [the sample `Main` contained in this library](src/main/scala/org/hammerlab/hadoop_bam/Main.scala):

```bash
sbt assembly
JAR=target/scala-2.11/hadoop-bam-assembly-1.0.0-SNAPSHOT.jar
time spark-submit "$JAR" -n 32 <BAM>
```

This print all computed splits as well as a timing stat for just the split-computation (whereas the stats output by `time` will include various app initialization-overhead time as well).


[hadoop-bam]: https://github.com/HadoopGenomics/Hadoop-BAM
[spark-bam]: https://github.com/hammerlab/spark-bam
