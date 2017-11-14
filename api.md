# API

With [spark-bam on the classpath][linking], a `SparkContext` can be "enriched" with relevant methods for loading BAM files by importing:
   
```scala
import spark_bam._
```

## [loadReads][`loadReads`]

The primary method exposed is `loadReads`, which will load an `RDD` of [HTSJDK `SAMRecord`s][`SAMRecord`] from a `.sam`, `.bam`, or `.cram` file:

```scala
sc.loadReads(path)
// RDD[SAMRecord]
```

Arguments:

- `path` (required)
	- an [`hammerlab.paths.Path`]
	- can be constructed [from a `URI`][Path URI ctor], [`String` (representing a `URI`)][Path String ctor], [or `java.nio.file.Path`][Path NIO ctor]:

		```scala
		import hammerlab.path._
		val path = Path("test_bams/src/main/resources/2.bam")
		```
- `bgzfBlocksToCheck`: optional (default: 5)
- `readsToCheck`: 
	- optional (default: 10)
	- number of consecutive reads to verify when determining a record/split boundary 
- `maxReadSize`: 
	- optional ()default: 10000000)
	- throw an exception if a record boundary is not found in this many (uncompressed) positions
- `splitSize`: 
	- optional (default: taken from underlying Hadoop filesystem APIs)
	- shorthands accepted, e.g. [`16m`, `32MB`](https://github.com/hammerlab/io-utils/blob/bytes-1.0.2/bytes/src/test/scala/org/hammerlab/bytes/BytesTest.scala#L19-L44)

## [loadBamIntervals][`loadBamIntervals`]

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
- `splitSize`: optional (default: taken from underlying Hadoop filesystem APIs)
- `estimatedCompressionRatio`
	- optional (default: `3.0`)
	- minor parameter used for approximately balancing Spark partitions; shouldn't be necessary to change

## [loadReadsAndPositions][`loadReadsAndPositions`]

Implementation of [`loadReads`][loadreads-section]: takes the same arguments, but returns [`SAMRecord`]s keyed by BGZF position ([`Pos`]).

Primarly useful for analyzing split-computations, e.g. in the [`compute-splits`] command.

## [loadSplitsAndReads][`loadSplitsAndReads`]

Similar to [`loadReads`][loadreads-section], but also returns computed [`Split`]s alongside the `RDD[SAMRecord]`.

Primarly useful for analyzing split-computations, e.g. in the [`compute-splits`] command.

[loadreads-section]: #loadreads


[`CanLoadBam`]: https://github.com/hammerlab/spark-bam/blob/master/load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala
[`loadReads`]: https://github.com/hammerlab/spark-bam/blob/master/load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#L352
[`loadBamIntervals`]: https://github.com/hammerlab/spark-bam/blob/master/load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#L62
[`loadReadsAndPositions`]: https://github.com/hammerlab/spark-bam/blob/master/load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#L285
[`loadSplitsAndReads`]: https://github.com/hammerlab/spark-bam/blob/master/load/src/main/scala/org/hammerlab/bam/spark/load/CanLoadBam.scala#L249

[`SAMRecord`]: https://github.com/samtools/htsjdk/blob/2.9.1/src/main/java/htsjdk/samtools/SAMRecord.java

[`LociSet`]: https://github.com/hammerlab/genomic-loci/blob/2.0.1/src/main/scala/org/hammerlab/genomics/loci/set/LociSet.scala

[`Pos`]: https://github.com/hammerlab/spark-bam/blob/master/bgzf/src/main/scala/org/hammerlab/bgzf/Pos.scala
[`Split`]: https://github.com/hammerlab/spark-bam/blob/master/check/src/main/scala/org/hammerlab/bam/spark/Split.scala

[`compute-splits`]: cli#compute-splits

[`org.hammerlab.paths.Path`]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala
[Path NIO ctor]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala#L14
[Path URI ctor]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala#L157
[Path String ctor]: https://github.com/hammerlab/path-utils/blob/1.2.0/src/main/scala/org/hammerlab/paths/Path.scala#L145-L155

[linking]: index#linking
