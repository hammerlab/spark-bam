package org.hammerlab.bam.check.simple

import org.hammerlab.bam.check
//import org.hammerlab.bgzf.Pos

trait Checker
  extends check.Checker[Boolean]

//class CompareChecker(checker1: check.Checker[Boolean], checker2: check.Checker[Boolean])
//  extends check.Checker[(Boolean, Boolean)] {
//  override def apply(pos: Pos): (Call, Call) = ???
//  override def close(): Unit = {
//    checker1.close()
//    checker2.close()
//  }
//}
