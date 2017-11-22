import org.apache.spark.SparkContext
import org.hammerlab.bam.spark.load.CanLoadBam
import org.hammerlab.spark.Context

package object spark_bam {
  implicit class LoadBamSparkContext(val sc: SparkContext)
    extends CanLoadBam

  implicit class LoadBamContext(val ctx: Context)
    extends CanLoadBam {
    override implicit def sc: SparkContext = ctx
  }
}
