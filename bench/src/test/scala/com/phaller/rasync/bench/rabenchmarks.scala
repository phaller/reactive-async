package com.phaller.rasync
package bench

import com.phaller.rasync.cell.CellCompleter
import com.phaller.rasync.lattice.lattices.{ NaturalNumberKey, NaturalNumberLattice }
import com.phaller.rasync.pool.HandlerPool
import lattice.Lattice
import org.scalameter.api._
import org.scalameter.picklers.noPickler._

import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._

object ReactiveAsyncBenchmarks extends PerformanceTest.Microbenchmark {
  /* configuration */
  override def executor = LocalExecutor(
    new Executor.Warmer.Default,
    Aggregator.min,
    new Measurer.Default)
  override def reporter = new LoggingReporter
  override def persistor = Persistor.None

  val nrOfCells = 100000
  val nrOfThreads = 8
  val size = Gen.single(s"$nrOfCells cells")(nrOfCells)

  /* lattice instance for cells */
  implicit val naturalNumberLattice: Lattice[Int] = new NaturalNumberLattice

  /* creation of cells/cell completers */
  performance of "Cells" in {
    measure method "creating" in {
      using(size) config (
        exec.benchRuns -> 9) in {
          r =>
            {
              implicit val pool = new HandlerPool(NaturalNumberKey, nrOfThreads)
              for (i <- 1 to r)
                pool.execute(() => { CellCompleter[Int, Null]() }: Unit)
              waitUntilQuiescent(pool)
            }
        }
    }
  }

  /* completion of cells */
  performance of "Cells" in {
    measure method "create and putFinal" in {
      using(size) config (
        exec.benchRuns -> 9) in {
          r =>
            {
              implicit val pool = new HandlerPool(NaturalNumberKey, nrOfThreads)
              for (i <- 1 to r) {
                pool.execute(() => {
                  val cellCompleter = CellCompleter[Int, Null]()
                  cellCompleter.putFinal(1)
                })
              }
              waitUntilQuiescent(pool)
            }
        }
    }
  }

  performance of "Cells" in {
    measure method "putNext" in {
      using(Gen.unit(s"$nrOfCells cells")) config (
        exec.benchRuns -> 9) in {
          (Unit) =>
            implicit val pool = new HandlerPool(NaturalNumberKey, nrOfThreads)
            val cellCompleter = CellCompleter[Int, Null]()
            for (i <- 1 to nrOfCells) pool.execute(() => cellCompleter.putNext(i))
            waitUntilQuiescent(pool)
        }
    }
  }

  def waitUntilQuiescent(pool: HandlerPool[_, _]): Unit = {
    val p = Promise[Boolean]
    pool.onQuiescent { () =>
      p.success(true)
    }
    Await.ready(p.future, 30.seconds)
  }
}
