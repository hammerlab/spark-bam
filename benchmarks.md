# Benchmarks

## Accuracy

### [spark-bam]

There are no known situations where [spark-bam] incorrectly classifies a BAM-record-boundary.

### [hadoop-bam]

[hadoop-bam] seems to have a false-positive rate of about 1 in every TODO uncompressed BAM positions.
 
In all such false-positives:
- the true start of a read is one byte further. That read is:
	- unmapped, and
	- "placed" in chromosome 1 between TODO and TODO
- the "invalid cigar operator" and "empty read name" checks would both correctly rule out the position as a record-start

#### Data

Some descriptions of BAMs that have been used for benchmarking:

- DREAM challenge
	- synthetic dataset 2
		- normal BAM: 156.1GB, 22489 false-positives
		- tumor BAM: 173.1GB, 0 false-positives

## Speed

TODO


<!-- Repos -->
[hadoop-bam]: https://github.com/HadoopGenomics/Hadoop-BAM
[spark-bam]: https://github.com/hammerlab/spark-bam
