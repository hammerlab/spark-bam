package org.hammerlab.hadoop_bam

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.hammerlab.hadoop_bam.bgzf.VirtualPos
import org.hammerlab.stats.Stats
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class IndexTest
  extends Suite {
  test("file") {
    val path = new Path(File("22gb.bam.bai").uri)
    val index = Index(path)

    val references = index.references
    references.size should be(85)

    val offsets = references.flatMap(_.offsets)
    offsets.size should be(189285)

    val allAddresses = index.allAddresses
    allAddresses.size should be(208762)

    Stats(
      allAddresses
        .map(_.blockPos)
        .sliding(2)
        .map(l ⇒ l(1) - l(0))
        .toVector
    )
    .toString should be(
      """num:	208761,	mean:	117056.8,	stddev:	362051.1,	mad:	0
        |elems:	0×114, 1, 0×15, 1×2, 0, 29, 15, 8, 7, 6, …, 92329, 70383, 0, 137102, 0, 205838, 22614, 0×6, 27454, 0×2
        |sorted:	0×130528, 1×6, 2, 3×2, 5×2, 6×2, 7×2, 8, 9, 13, …, 13459301, 14420823, 14637952, 14961000, 16714778, 16858324, 17307262, 20117331, 21270145, 31318741
        |0.001:	0
        |0.01:	0
        |0.1:	0
        |1:	0
        |5:	0
        |10:	0
        |25:	0
        |50:	0
        |75:	48250
        |90:	382788
        |95:	657823
        |99:	1431315.8
        |99.9:	3665724.6
        |99.99:	10446035.7
        |99.999:	19871169.0
        |"""
      .stripMargin
      .trim
    )

    val chr1 = references.head

    val chr1Offsets = chr1.offsets
    chr1Offsets.size should be(15213)

    chr1Offsets(0) should be(VirtualPos(  5322,  8020))
    chr1Offsets(1) should be(VirtualPos(168780, 55019))
  }

//  test("read offset") {
//    val pos = 1769520335L
//    val conf = new Configuration
//    val path = new Path("file:///Users/ryan/c/hl/hadoop-bam/B0433_N.bam")
//    val fs = path.getFileSystem(conf)
//    val is = fs.open(path)
//    is.seek(pos)
//    is.read()
//  }
}
