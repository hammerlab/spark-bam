package org.hammerlab

import org.hammerlab.test.resources.File

package object resources {
  val bam5k = File("5k.bam")
  val tcgaBamExcerpt = File("1.2203053-2211029.bam")
  val tcgaBamExcerptUnindexed = File("1.2203053-2211029.noblocks.bam")
}
