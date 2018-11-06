//package com.phaller.rasync.npv
//
//import scala.concurrent.{ Promise, Future, ExecutionContext }
//
//object NpvTask {
//
//  def calculateMin(flows: Seq[Distribution], rate: Distribution): Double = {
//    val minFlows = Array.ofDim[Double](flows.length)
//    for (i <- 0 until flows.length) {
//      minFlows(i) = flows(i).getMin()
//    }
//    NetPresentValue.npv(minFlows, rate.getMax())
//  }
//
//  def calculateMax(flows: Seq[Distribution], rate: Distribution): Double = {
//    val maxFlows = Array.ofDim[Double](flows.length)
//    for (i <- 0 until flows.length) {
//      maxFlows(i) = flows(i).getMax()
//    }
//    NetPresentValue.npv(maxFlows, rate.getMin())
//  }
//
//}
//
//class NpvTask(p: Promise[StatsCollector], min: Double, max: Double, numBuckets: Int, numIterations: Int, rate: Distribution, flows: Distribution*)(implicit ctx: ExecutionContext) extends AbstractNpvTask {
//
//  def this(p: Promise[StatsCollector], numBuckets: Int, numIterations: Int, rate: Distribution, flows: Distribution*)(implicit ctx: ExecutionContext) {
//    this(p, NpvTask.calculateMin(flows, rate), NpvTask.calculateMax(flows, rate), numBuckets, numIterations, rate, flows: _*)
//  }
//
//  private def sampleFlows(): Array[Double] = {
//    val sample = Array.ofDim[Double](flows.length)
//    for (i <- 0 until flows.length) {
//      sample(i) = flows(i).sample()
//    }
//    sample
//  }
//
//  def run(): Unit = {
//    val children =
//      if (numChunks < 0) calcNumChunks(numIterations) else numChunks
//
//    if (numIterations <= minChunkSize || children == 1) {
//      val collector = new StatsCollector(min, max, numBuckets)
//      for (i <- 0 until numIterations) {
//        collector.addObs(NetPresentValue.npv(sampleFlows(), rate.sample()))
//      }
//      p.success(collector)
//    } else {
//      var promises: List[Promise[StatsCollector]] = List()
//      for (i <- 0 until children) {
//        val statsPromise = Promise[StatsCollector]()
//        val subTask = new NpvTask(statsPromise, min, max, numBuckets, numIterations / children, rate, flows: _*)
//        subTask.setMinChunkSize(minChunkSize)
//        subTask.setNumChunks(numChunks)
//        promises = statsPromise :: promises
//        ctx.execute(subTask)
//      }
//      val subFutures = promises.map(_.future)
//      Future.sequence(subFutures).map { listOfCollectors =>
//        val collector = new StatsCollector(min, max, numBuckets)
//        for (subCollector <- listOfCollectors) {
//          collector.combine(subCollector)
//        }
//        p.success(collector)
//      }
//    }
//  }
//
//}
