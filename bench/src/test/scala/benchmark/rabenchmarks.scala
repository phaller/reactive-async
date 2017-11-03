import cell.{ CellCompleter, HandlerPool }
import lattice.{ Lattice, NaturalNumberLattice, NaturalNumberKey }

import scala.annotation.tailrec

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
              val pool = new HandlerPool(nrOfThreads)
              for (i <- 1 to r)
                pool.execute(() => { CellCompleter[NaturalNumberKey.type, Int](pool, NaturalNumberKey) }: Unit)
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
              val pool = new HandlerPool(nrOfThreads)
              for (i <- 1 to r) {
                pool.execute(() => {
                  val cellCompleter = CellCompleter[NaturalNumberKey.type, Int](pool, NaturalNumberKey)
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
            val pool = new HandlerPool(nrOfThreads)
            val cellCompleter = CellCompleter[NaturalNumberKey.type, Int](pool, NaturalNumberKey)
            for (i <- 1 to nrOfCells) pool.execute(() => cellCompleter.putNext(i))
            waitUntilQuiescent(pool)
        }
    }
  }

  def waitUntilQuiescent(pool: HandlerPool): Unit = {
    val p = Promise[Boolean]
    pool.onQuiescent { () =>
      p.success(true)
    }
    Await.ready(p.future, 30.seconds)
  }
}
