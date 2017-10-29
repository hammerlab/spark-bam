package org.hammerlab.args

import caseapp.{ ValueDescription, HelpMessage ⇒ M, Name ⇒ O }
import org.hammerlab.bgzf.block.BGZFBlocksToCheck

case class FindBlockArgs(
    @O("z")
    @ValueDescription("num=5")
    @M("When searching for BGZF-block boundaries, look this many blocks ahead to verify that a candidate is a valid block. In general, probability of a false-positive is 2^(-32N) for N blocks of look-ahead")
    bgzfBlocksToCheck: BGZFBlocksToCheck
)
