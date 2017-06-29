package org.hammerlab.io

case class SampleSize(size: Option[Int]) {
  def <(other: Long): Boolean =
    size.exists(_ < other)
}
