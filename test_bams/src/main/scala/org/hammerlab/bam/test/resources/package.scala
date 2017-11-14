package org.hammerlab.bam.test

import hammerlab.path._
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.Url

package object resources {

  def path(name: String): Path = Path(Url(name).toURI)

  val bam1 = path("1.bam")
  val bam1Unindexed = path("1.noblocks.bam")
  val bam1BlockAligned = path("1.block-aligned.bam")

  val bam2 = path("2.bam")
  val sam2 = path("2.sam")

  trait TestBams {
    self: Suite ⇒

    def sam(bam: Path): Path =
      Path(bam.toString.dropRight(3) + "sam")
    
    def copyBamAndMetadata(bam: Path): Path = {

      val dir = tmpDir()
      val path = fileCopy(bam, dir / bam.basename)
      
      val extensions = 
        "blocks" :: 
          "records" ::
          "bai" :: 
          Nil
      
      for {
        extension ← extensions
        extendedPath = bam + s".$extension"
        if extendedPath.exists
        newPath = path + s".$extension"
      } {
        fileCopy(extendedPath, newPath)
      }
      
      val samPath = sam(bam)
      if (samPath.exists)
        fileCopy(samPath, dir / samPath.basename)
      
      path
    }
    
    /** 
     * Lazily make copies of a few BAMs and their associated "blocks" and "records" files, so that tests can resolve 
     * relative paths to the latter.
     * 
     * Hadoop Paths don't deal with URIs correctly, so relative paths inside JARs can't be resolved.
     */
    lazy val bam1 = copyBamAndMetadata(resources.bam1)
    lazy val bam2 = copyBamAndMetadata(resources.bam2)
    lazy val sam2 = sam(bam2)
  }
}
