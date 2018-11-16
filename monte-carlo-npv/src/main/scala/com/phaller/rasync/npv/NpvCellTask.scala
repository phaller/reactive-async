package com.phaller.rasync.npv

import com.phaller.rasync.lattice.{ DefaultKey, Lattice, NotMonotonicException }
import com.phaller.rasync.{ Cell, CellCompleter, HandlerPool }

import scala.util.{ Success, Failure }

abstract class AbstractNpvTask extends Runnable {

  protected var minChunkSize: Int = 100
  protected var numChunks: Int = 2

  def setMinChunkSize(minChunkSize: Int): Unit = {
    this.minChunkSize = minChunkSize
  }

  def setNumChunks(numChunks: Int): Unit = {
    this.numChunks = numChunks
  }

  def calcNumChunks(n: Int): Int = {
    val nc: Int = Math.ceil(Math.sqrt(n / minChunkSize)).asInstanceOf[Int]
    nc
  }
}

// trivial lattice
class StatsLattice extends Lattice[StatsCollector] {
  override val bottom: StatsCollector = null
  override def join(current: StatsCollector, next: StatsCollector): StatsCollector = {
    if (current == null) next
    else throw NotMonotonicException(current, next)
  }
}

class NpvCellTask(p: CellCompleter[DefaultKey[StatsCollector], StatsCollector], min: Double, max: Double, numBuckets: Int, numIterations: Int, rate: Distribution, flows: Distribution*)(implicit pool: HandlerPool) extends AbstractNpvTask {

  type K = DefaultKey[StatsCollector]

  implicit val lattice: Lattice[StatsCollector] = new StatsLattice

  def this(p: CellCompleter[DefaultKey[StatsCollector], StatsCollector], numBuckets: Int, numIterations: Int, rate: Distribution, flows: Distribution*)(implicit pool: HandlerPool) {
    this(p, NpvTask.calculateMin(flows, rate), NpvTask.calculateMax(flows, rate), numBuckets, numIterations, rate, flows: _*)
  }

  private def sampleFlows(): Array[Double] = {
    val sample = Array.ofDim[Double](flows.length)
    for (i <- 0 until flows.length) {
      sample(i) = flows(i).sample()
    }
    sample
  }

  def run(): Unit = {
    val children =
      if (numChunks < 0) calcNumChunks(numIterations) else numChunks

    if (numIterations <= minChunkSize || children == 1) {
      val collector = new StatsCollector(min, max, numBuckets)
      for (i <- 0 until numIterations) {
        collector.addObs(NetPresentValue.npv(sampleFlows(), rate.sample()))
      }
      p.putFinal(collector)
    } else {
      var completers: List[CellCompleter[K, StatsCollector]] = List()
      for (i <- 0 until children) {
        val statsCompleter =
          CellCompleter[K, StatsCollector](new DefaultKey[StatsCollector])
        val subTask = new NpvCellTask(statsCompleter, min, max, numBuckets, numIterations / children, rate, flows: _*)
        subTask.setMinChunkSize(minChunkSize)
        subTask.setNumChunks(numChunks)
        completers = statsCompleter :: completers
        pool.execute(subTask)
      }
      val subCells = completers.map(_.cell)
      Cell.sequence(subCells).onComplete {
        case Success(listOfCollectors) =>
          val collector = new StatsCollector(min, max, numBuckets)
          for (subCollector <- listOfCollectors) {
            collector.combine(subCollector)
          }
          p.putFinal(collector)
        case f @ Failure(_) =>
          p.tryComplete(f.asInstanceOf[Failure[StatsCollector]], None)
      }
    }
  }

}
