import scala.concurrent.Promise
import scala.annotation.tailrec
import org.scalameter.api._
import org.scalameter.picklers.noPickler._

object FuturesAndPromisesBenchmarks extends PerformanceTest.Microbenchmark {
  /* configuration */
  override def executor = LocalExecutor(
    new Executor.Warmer.Default,
    Aggregator.min,
    new Measurer.Default)
  override def reporter = new LoggingReporter
  override def persistor = Persistor.None

  val nrOfPromises = 100000
  val size = Gen.single("Number Of Promises")(nrOfPromises)

  /* creation of promises */
  performance of "Promises" in {
    measure method "creating" in {
      using(size) config (
        exec.benchRuns -> 9) in {
          r => for (i <- 1 to r) Promise[Int]()
        }
    }
  }

  /* creation and completion of futures */
  performance of "Promises" in {
    measure method "creating and completing" in {
      using(size) config (
        exec.benchRuns -> 9) in {
          r =>
            for (i <- 1 to r) {
              val p = Promise[Int]
              p.success(1)
            }
        }
    }
  }

  /* refinement of promises */
  performance of "Promises" in {
    measure method "refinement" in {
      using(Gen.unit(s"$nrOfPromises promises")) config (
        exec.benchRuns -> 9) in {
          (Unit) =>
            {
              var i = 0
              val promises = createListPromises(nrOfPromises, List.empty)
              for (p <- promises) {
                i = i + 1
                p.success(i)
              }
            }
        }
    }
  }

  @tailrec
  def createListPromises(amount: Int, promises: List[Promise[Int]]): List[Promise[Int]] = {
    val p = Promise[Int]
    if (amount == 0) p :: promises
    else createListPromises(amount - 1, p :: promises)
  }
}
