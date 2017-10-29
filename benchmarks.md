# Benchmarks

## Accuracy

[spark-bam] and [hadoop-bam] were compared on the following datasets:

- 1000 Genomes: 72 bams, 559GB
	- {mapped, unmapped} x {low coverage, exome} for each of 18 individuals (HG000096 through HG000115, excepting HG000098 and HG000104)
- Genome in a Bottle: 3 bams, 192GB
	- [Ashkenazim trio, son (NA24385), Pacbio](ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/HG002_NA24385_son/PacBio_MtSinai_NIST/MtSinai_blasr_bam_GRCh37/): chromosomes X and Y
	- [NA12878 Pacbio WGS BAM](ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/NA12878/NA12878_PacBio_MtSinai/sorted_final_merged.bam)
- TCGA Lung-Cancer samples: 1060 bams, 14TB 
- DREAM challenge
	- synthetic data: 10 bams, 1.8TB
	- real data: 116 bams, 3.7TB

Across these datasets:
- [hadoop-bam] called false positives at uncompressed positions at a rate of between 1.60e-9 and 5.39e-5 
- this translated to an uncompressed "incorrect split" rate between 0 and 1.97e-4
	- many BAMs in the wild have reads aligned to BGZF-block boundaries, basically eliminating the chance of [hadoop-bam] calling a false positive
	- the highest incorrect-split rate, 1.97e-4, was observed on the Genome in a Bottle long-read data 

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
