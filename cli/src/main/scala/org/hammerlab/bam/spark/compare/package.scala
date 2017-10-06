package org.hammerlab.bam.spark

import java.lang.System.currentTimeMillis

import org.apache.hadoop.fs
import org.apache.hadoop.mapreduce.lib.input
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths
import org.apache.hadoop.mapreduce.{ InputSplit, Job }
import org.hammerlab.bam.check.{ MaxReadSize, ReadsToCheck }
import org.hammerlab.bam.header.Header
import org.hammerlab.bam.spark.load.Channels
import org.hammerlab.bgzf.Pos
import org.hammerlab.bgzf.block.{ BGZFBlocksToCheck, FindBlockStart }
import org.hammerlab.hadoop.Configuration
import org.hammerlab.hadoop.splits.{ FileSplit, FileSplits, MaxSplitSize }
import org.hammerlab.iterator.sliding.Sliding2Iterator._
import org.hammerlab.iterator.sorted.OrZipIterator._
import org.hammerlab.paths.Path
import org.hammerlab.types.{ Both, L, R }
import org.seqdoop.hadoop_bam.{ BAMInputFormat, FileVirtualSplit }
import shapeless.Generic

import scala.collection.JavaConverters._

package object compare {


}
