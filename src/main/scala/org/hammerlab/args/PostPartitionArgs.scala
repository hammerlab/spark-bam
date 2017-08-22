package org.hammerlab.args

import caseapp.{ ValueDescription, ExtraName ⇒ O, HelpMessage ⇒ M }

case class PostPartitionArgs(
    @O("p")
    @ValueDescription("num=100000")
    @M("After running eager and/or seqdoop checkers over a BAM file and filtering to just the contested positions, repartition to have this many records per partition. Typically there are far fewer records at this stage, so it's useful to coalesce down to avoid 1,000's of empty partitions")
    resultsPerPartition: Int = 100000
)
