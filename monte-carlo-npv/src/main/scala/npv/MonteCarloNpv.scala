package npv

import scala.concurrent.{Promise, Await, ExecutionContext}
import scala.concurrent.duration._

import java.util.concurrent.{CountDownLatch, ForkJoinPool}


class MonteCarloNpv {
  import MonteCarloNpv._

  private val initial: Distribution = new SingleValueDistribution(-20000)
  private val year1: Distribution   = new TriangleDistribution(0, 4000, 10000)
  private val year2: Distribution   = new TriangleDistribution(0, 4000, 10000)
  private val year3: Distribution   = new TriangleDistribution(1000, 8000, 20000)
  private val year4: Distribution   = new TriangleDistribution(1000, 8000, 20000)
  private val year5: Distribution   = new TriangleDistribution(5000, 12000, 40000)
  private val rate: Distribution    = new TriangleDistribution(2, 4, 8)

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

  def parallel(minChunkSize: Int, numChunks: Int): StatsCollector = {
    implicit val ctx =
      ExecutionContext.fromExecutorService(new ForkJoinPool(numThreads))
    val p = Promise[StatsCollector]()
    val task =
      new NpvTask(p, 10, NUM_ITER, rate, initial, year1, year2, year3, year4, year5)
    task.setMinChunkSize(minChunkSize)
    task.setNumChunks(numChunks)
    ctx.execute(task)
    Await.result(p.future, 600.seconds)
  }

}

object MonteCarloNpv {
  private val NUM_ITER = 10000000
  private var numThreads = 0

  private def oneSize(name: String, children: Int, chunkSize: Int, npv: MonteCarloNpv): Long = {
    val swName: String = name + " (children=" + children + ", min fork size=" + chunkSize + ")"
    println(swName)
    val start = System.nanoTime()
    val stats = npv.parallel(chunkSize, children)
    val end = System.nanoTime()
    println(stats)
    end - start
  }

  def main(args: Array[String]): Unit = {
    numThreads = args(0).toInt
    println(s"Using $numThreads threads")

    val npv = new MonteCarloNpv()

    val startSeq = System.nanoTime()
    val stats = npv.sequential()
    val endSeq = System.nanoTime()
    println(stats)

    println(s"Time (sequential): ${(endSeq - startSeq) / 1000000} ms")

    val timeFut = oneSize("Futures", 2, 500, npv)
    println(s"Time (futures): ${timeFut / 1000000} ms")
  }

}
