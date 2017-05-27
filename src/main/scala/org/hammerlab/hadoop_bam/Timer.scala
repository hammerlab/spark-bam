package org.hammerlab.hadoop_bam

abstract class StoppableThread extends Thread {
  protected var stopped = false
  def end(): Unit = {
    stopped = true
  }
}

object Timer {
  def time[T](fn: () ⇒ Unit,
              bodyFn: ⇒ T,
              intervalS: Int = 1): T = {
    val thread =
      new StoppableThread {
        override def run(): Unit = {
          while (!stopped) {
            Thread.sleep(intervalS * 1000)
            fn()
          }
        }
      }

    thread.start()

    val ret = bodyFn

    thread.end()

    ret
  }
}
