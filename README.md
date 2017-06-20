# spark-bam
[![Build Status](https://travis-ci.org/hammerlab/spark-bam.svg?branch=master)](https://travis-ci.org/hammerlab/spark-bam)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/spark-bam/badge.svg?branch=master)](https://coveralls.io/github/hammerlab/spark-bam?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/org.hammerlab/spark-bam_2.11.svg?maxAge=600)](http://search.maven.org/#search%7Cga%7C1%7Cspark-bam)

Load [BAM files](http://samtools.github.io/hts-specs/SAMv1.pdf) using [Apache Spark](https://spark.apache.org/) and [HTSJDK](https://github.com/samtools/htsjdk).

Inspired by [HadoopGenomics/hadoop-bam](https://github.com/HadoopGenomics/Hadoop-BAM).

*Timings from `time spark-submit $JAR -n <NUM> <BAM>` for various numbers of threads (`NUM`), against a 178GB `BAM`; [raw data here](https://docs.google.com/spreadsheets/d/11c6T-HxR7bMdPOeS6l3n4klBuC9PhgrR5JcSg2qa_H4/edit#gid=1917204057).*

## Impetus

With hadoop-bam's [BAMInputFormat](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.8.0/src/main/java/org/seqdoop/hadoop_bam/BAMInputFormat.java), computing splits on a Google Cloud Storage (GCS)-resident BAM typically took O(minutes), e.g. 2mins for a 20GB BAM and 15mins on a 178GB BAM in recent benchmarks.

During this time, the driver node fetches data from each split-offset returned by `FileInputFormat`, typically ≈4 64KB BGZF blocks (or 256KB) every 32MB, 64MB, or 128MB according to the HDFS-block-size being used (or simulated in the case of a [`GoogleHadoopFS`](https://github.com/GoogleCloudPlatform/bigdata-interop/blob/v1.6.1/gcs/src/main/java/com/google/cloud/hadoop/fs/gcs/GoogleHadoopFS.java)). 

Each network request and resulting split-computation happens in serial, and the network requests exhibit a high, fixed latency-cost.
 
By having N threads parallelize the work of [fetching the start of each `FileInputFormat`-split], near-perfect linear scaling has been observed, up to 64 threads bringing split-computation from 15mins to 20s on a 178GB BAM and from 2mins to 4s on a 22GB BAM; both were benchmarked on a 4-core GCE `n1-standard-4` VM.

## Using

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


