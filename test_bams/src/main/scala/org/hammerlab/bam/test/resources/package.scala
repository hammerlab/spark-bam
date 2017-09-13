package org.hammerlab.bam.test

import java.nio.file.Files

import org.hammerlab.paths.Path
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.Url

package object resources {
  
  def path(name: String): Path = Path(Url(name).toURI)  
  
  val bam5k = path("5k.bam")
  val sam5k = path("5k.sam")
  val tcgaBamExcerpt = path("1.2203053-2211029.bam")
  val tcgaBamExcerptUnindexed = path("1.2203053-2211029.noblocks.bam")

  trait TestBams {
    self: Suite ⇒

    def fileCopy(path: Path, out: Path): Path = {
      val in = path.inputStream
      Files.copy(in, out)
      in.close()
      out
    }

    def fileCopy(path: Path): Path = {
      val out = tmpPath(suffix = s".${path.extension}")
      val in = path.inputStream
      Files.copy(in, out)
      in.close()
      out
    }

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
    lazy val bam5k = copyBamAndMetadata(resources.bam5k)
    lazy val sam5k = sam(bam5k)
    lazy val tcgaBamExcerpt = copyBamAndMetadata(resources.tcgaBamExcerpt)
    
  }
}
