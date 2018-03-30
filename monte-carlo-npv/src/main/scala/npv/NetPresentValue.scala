package com.phaller.rasync.npv

object NetPresentValue {

  def npv(flows: Array[Double], discountRate: Double): Double = {
    var result: Double = 0
    val rate: Double = 1 + (discountRate * 0.01)
    for (i <- 0 until flows.length) {
      result = result + (flows(i) / Math.pow(rate, i))
    }
    result
  }

}
