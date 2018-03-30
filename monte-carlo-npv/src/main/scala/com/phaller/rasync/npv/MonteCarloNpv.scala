package com.phaller.rasync.npv

import scala.concurrent.{ Promise, Await, ExecutionContext }
import scala.concurrent.duration._

import scala.util.{ Success, Failure }

import java.util.concurrent.{ CountDownLatch, ForkJoinPool }
import java.util.concurrent.atomic.AtomicReference

import com.phaller.rasync.{ HandlerPool, CellCompleter }
import com.phaller.rasync.lattice.{ Lattice, DefaultKey }

class MonteCarloNpv {
  import MonteCarloNpv._

  private val initial: Distribution = new SingleValueDistribution(-20000)
  private val year1: Distribution = new TriangleDistribution(0, 4000, 10000)
  private val year2: Distribution = new TriangleDistribution(0, 4000, 10000)
  private val year3: Distribution = new TriangleDistribution(1000, 8000, 20000)
  private val year4: Distribution = new TriangleDistribution(1000, 8000, 20000)
  private val year5: Distribution = new TriangleDistribution(5000, 12000, 40000)
  private val rate: Distribution = new TriangleDistribution(2, 4, 8)

  def sequential(): StatsCollector = {
    implicit val ctx =
      ExecutionContext.fromExecutorService(new ForkJoinPool(numThreads))
    val p = Promise[StatsCollector]()
    val task =
      new NpvTask(p, 10, NUM_ITER, rate, initial, year1, year2, year3, year4, year5)
    task.setMinChunkSize(NUM_ITER + 1)
    task.run()
    Await.result(p.future, 600.seconds)
  }

  def parallel(minChunkSize: Int, numChunks: Int)(implicit ctx: ExecutionContext): StatsCollector = {
    val p = Promise[StatsCollector]()
    val task =
      new NpvTask(p, 10, NUM_ITER, rate, initial, year1, year2, year3, year4, year5)
    task.setMinChunkSize(minChunkSize)
    task.setNumChunks(numChunks)
    ctx.execute(task)
    Await.result(p.future, 600.seconds)
  }

  def cell(minChunkSize: Int, numChunks: Int)(implicit pool: HandlerPool): StatsCollector = {
    implicit val lattice: Lattice[StatsCollector] = new StatsLattice
    val p = CellCompleter[DefaultKey[StatsCollector], StatsCollector](new DefaultKey[StatsCollector])
    val task =
      new NpvCellTask(p, 10, NUM_ITER, rate, initial, year1, year2, year3, year4, year5)
    task.setMinChunkSize(minChunkSize)
    task.setNumChunks(numChunks)
    pool.execute(task)
    val latch = new CountDownLatch(1)
    val atomic = new AtomicReference[StatsCollector]
    p.cell.onComplete {
      case Success(x) =>
        atomic.lazySet(x)
        latch.countDown()
      case Failure(_) =>
        atomic.lazySet(null)
        latch.countDown()
    }
    latch.await()
    atomic.get()
  }

}

object MonteCarloNpv {
  private val NUM_ITER = 10000000
  private var numThreads = 0

  private def oneSize(name: String, children: Int, chunkSize: Int, npv: MonteCarloNpv): Long = {
    val swName: String = name + " (children=" + children + ", min fork size=" + chunkSize + ")"
    println(swName)
    implicit val ctx =
      ExecutionContext.fromExecutorService(new ForkJoinPool(numThreads))
    val start = System.nanoTime()
    val stats = npv.parallel(chunkSize, children)
    val end = System.nanoTime()
    // println(stats)
    end - start
  }

  private def oneSizeCell(name: String, children: Int, chunkSize: Int, npv: MonteCarloNpv): Long = {
    val swName: String = name + " (children=" + children + ", min fork size=" + chunkSize + ")"
    println(swName)
    implicit val pool = new HandlerPool(numThreads)
    val start = System.nanoTime()
    val stats = npv.cell(chunkSize, children)
    val end = System.nanoTime()
    // println(stats)
    pool.shutdown()
    end - start
  }

  def main(args: Array[String]): Unit = {
    numThreads = args(0).toInt
    println(s"Using $numThreads threads")

    val npv = new MonteCarloNpv()

    val startSeq = System.nanoTime()
    val stats = npv.sequential()
    val endSeq = System.nanoTime()
    // println(stats)

    println(s"Time (sequential): ${(endSeq - startSeq) / 1000000} ms")

    val timeCell = oneSizeCell("Cells", 2, 500, npv)
    println(s"Time (cells): ${timeCell / 1000000} ms")

    val timeFut = oneSize("Futures", 2, 500, npv)
    println(s"Time (futures): ${timeFut / 1000000} ms")
  }

}
