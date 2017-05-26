package org.hammerlab.hadoop_bam

object Timer {
  def time(fn: () ⇒ Unit,
           intervalS: Int = 1,
           stopFn: () => Boolean = () ⇒ false): Thread = {
    val thread =
      new Thread {
        override def run(): Unit = {
          while (!stopFn()) {
            Thread.sleep(intervalS * 1000)
            fn()
          }
        }
      }

    thread.start()

    thread
  }
}
