# Benchmarks

## Data

[spark-bam] and [hadoop-bam] were compared on the following datasets:

- 1000 Genomes: 72 bams, 559GB
	- {mapped, unmapped} x {low coverage, exome} for each of 18 individuals (HG000096 through HG000115, excepting HG000098 and HG000104)
- Genome in a Bottle (GiaB): 3 bams, 192GB
	- [Ashkenazim trio, son (NA24385), Pacbio](ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/HG002_NA24385_son/PacBio_MtSinai_NIST/MtSinai_blasr_bam_GRCh37/): chromosomes X and Y
	- [NA12878 Pacbio WGS BAM](ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/NA12878/NA12878_PacBio_MtSinai/sorted_final_merged.bam)
- TCGA Lung-Cancer samples: 1060 bams, 14TB 
- DREAM challenge
	- synthetic data: 10 bams, 1.8TB
	- real data: 116 bams, 3.7TB

## Accuracy

Raw data for the above can be found in [this google sheet][results sheet]. 

Across the datasets described above:

- [hadoop-bam] called false positives at uncompressed positions at a rate of between 1.60e-9 and 5.39e-5 
- this translated to an "incorrect split" rate between 0 and 1.97e-4, i.e. up to 2 out of 10000 splits
	- many BAMs in the wild have reads aligned to BGZF-block boundaries, basically eliminating the chance of [hadoop-bam] calling a false positive
	- the highest incorrect-split rate, 1.97e-4, was observed on the Genome in a Bottle long-read data 

### [spark-bam]

There are no known situations where [spark-bam] incorrectly classifies a BAM-record-boundary.

### [hadoop-bam]

On the above data [hadoop-bam] exhibited false-positive rates between 1 per 18k and 1 per 625MM uncompressed BAM positions.
 
False-positives were discovered that were only correctly identified in [spark-bam] due to each of [the additional checks in spark-bam][checks table]:

In addition, several hundred false-negatives were discovered in GiaB PacBio long-read data: [hadoop-bam] missed sites that are true read-starts. None of these sites were on split boundaries, but it seems likely that correctness errors could ensue if they were.

## Speed

Four CLI commands compare [spark-bam]'s speed with [hadoop-bam]'s in various ways:

- [`time-load`]
- [`count-reads`]
- [`compute-splits`]
- [`compare-splits`]

The latter two time a single CPU computing a split, which [hadoop-bam] is much faster at, but the former two better factor in [spark-bam]'s gains from parallelization.

In particular, [`time-load`] does minimal work other than split-computation, returning the first read from every partition, so [spark-bam] is much faster than [hadoop-bam]. [`count-reads`] amortizes [spark-bam]'s split-computation edge over more subsequent work, so the difference is less pronounced.

### DREAM Synthetic BAM Benchmarks

[`count-reads`] and [`time-load`] were each run, with and without the `-s` flag (i.e. with each of [spark-bam] and [hadoop-bam] running first and incurring Spark-setup issues, colder caches, etc.), on the 10 DREAM synthetic BAMs described [above](#data).

- [`count-reads`]:
	- [spark-bam] first: [spark-bam] 2.1x-4.7x faster
	- [hadoop-bam] first: [spark-bam] 3x-13x faster
- [`time-load`]: 
	- [spark-bam] first: [spark-bam] 5.5x-10.8x faster
	- [hadoop-bam] first: [spark-bam] 17x-57x faster


<!-- Repos -->
[hadoop-bam]: https://github.com/HadoopGenomics/Hadoop-BAM
[spark-bam]: https://github.com/hammerlab/spark-bam

[results sheet]: https://docs.google.com/a/hammerlab.org/spreadsheets/d/1TOF8BbSFcdTCAl86q7TqSPh3zPFcbl-krvABE9QLEfo/edit?usp=sharing

[checks table]: ./motivation#improved-record-boundary-detection-robustness

[`time-load`]: ./command-line#time-load
[`count-reads`]: ./command-line#count-reads
[`compute-splits`]: ./command-line#compute-splits
[`compare-splits`]: ./command-line#compare-splits
