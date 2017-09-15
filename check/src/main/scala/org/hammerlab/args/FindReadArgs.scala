package org.hammerlab.args

import caseapp.{ ValueDescription, HelpMessage ⇒ M }
import org.hammerlab.bam.check.Checker.{ MaxReadSize, ReadsToCheck }

case class FindReadArgs(
    @ValueDescription("num=10000000")
    @M("Maximum number of bases long for spark-bam checkers to allow a read to be")
    maxReadSize: MaxReadSize,

    @ValueDescription("num=10")
    @M("Number of consecutive reads to check for validity before declaring a position to be read-start")
    readsToCheck: ReadsToCheck
)
