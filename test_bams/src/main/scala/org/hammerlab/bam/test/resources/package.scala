package org.hammerlab.bam.test

import org.hammerlab.paths.Path
import org.hammerlab.test.resources.Url

package object resources {
  val bam5k = Path(Url("5k.bam").toURI)
  val tcgaBamExcerpt = Path(Url("1.2203053-2211029.bam").toURI)
  val tcgaBamExcerptUnindexed = Path(Url("1.2203053-2211029.noblocks.bam").toURI)
}
