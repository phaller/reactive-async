package com.phaller.rasync.npv

import java.util.concurrent.ThreadLocalRandom


trait Distribution {
  def sample(): Double
  def getMax(): Double
  def getMin(): Double
}

class SingleValueDistribution(value: Double) extends Distribution {
  override def sample(): Double = value
  override def getMax(): Double = value
  override def getMin(): Double = value
}

class TriangleDistribution(min: Double, likely: Double, max: Double)
        extends Distribution {

  assert(max >= likely)
  assert(likely >= min)

  val fc: Double = (likely - min) / (max - min)

  override def sample(): Double = {
    val u = ThreadLocalRandom.current().nextDouble()
    if (u < fc) {
      min + Math.sqrt(u * (max - min) * (likely - min))
    } else {
      max - Math.sqrt((1 - u) * (max - min) * (max - likely))
    }
  }

  override def getMin(): Double = min

  def getLikely(): Double = likely

  override def getMax(): Double = max

}
