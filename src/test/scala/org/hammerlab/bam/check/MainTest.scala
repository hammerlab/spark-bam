package org.hammerlab.bam.check

import java.lang.System.setProperty

import caseapp.RemainingArgs
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class MainTest
  extends Suite {
  test("compare mode") {
    val outputPath = tmpPath()

    setProperty("spark.master", "local[4]")
    setProperty("spark.app.name", getClass.getCanonicalName)
    setProperty("spark.driver.allowMultipleContexts", "true")
    setProperty("spark.ui.enabled", "false")

    Main.run(
      Args(
        blocksPerPartition = 5,
        eagerChecker = true,
        seqdoopChecker = true,
        outputPath = Some(outputPath.path.toString)
      ),
      RemainingArgs(
        Seq(File("1.2205029-2209029.bam")),
        Nil
      )
    )

    outputPath.read should be(
      """Seqdoop-only calls:
        |	155201:4462
        |	155201:4780
        |	155201:5097
        |	155201:5732
        |	155201:6365
        |	155201:6683
        |	155201:7977
        |	155201:8617
        |	155201:9580
        |	155201:10536
        |	155201:10852
        |	155201:11169
        |	155201:11808
        |	155201:13075
        |	155201:16890
        |	155201:18170
        |	155201:18443
        |	155201:19072
        |	155201:19389
        |	155201:21613
        |	225622:48936
        |	225622:49212"""
        .stripMargin
    )
  }
}
