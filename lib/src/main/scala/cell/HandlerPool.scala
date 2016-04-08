package cell

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.{Future, Promise}

import lattice.Key


class HandlerPool(parallelism: Int = 8) {

  private val pool: ForkJoinPool = new ForkJoinPool(parallelism)

  private val quiescentHandlers = new AtomicReference[List[() => Unit]](List())

  private val cellsNotDone = new AtomicReference[List[Cell[_, _]]](List())

  private val t = {
    val tmp = new MonitoringThread
    tmp.start()
    tmp
  }

  private class MonitoringThread extends Thread {
    private val backoff = Backoff()
    @volatile var terminate = false

    override def run(): Unit = {
      // periodically check whether fork/join pool is quiescent
      while (!terminate) {
        if (pool.isQuiescent) {
          // schedule registered quiescent handlers
          val handlers = quiescentHandlers.get()
          quiescentHandlers.compareAndSet(handlers, List())
          handlers.foreach { handler =>
            pool.execute(new Runnable { def run(): Unit = handler() })
          }
        } else
          backoff.once() // exponential back-off
      }
    }
  }

  def onQuiescent(handler: () => Unit): Unit = {
    val handlers = quiescentHandlers.get()
    // add handler
    val newHandlers = handler :: handlers
    quiescentHandlers.compareAndSet(handlers, newHandlers)
  }

  def register[K <: Key[V], V](cell: Cell[K, V]): Unit = {
    val registered = cellsNotDone.get()
    val newRegistered = cell :: registered
    cellsNotDone.compareAndSet(registered, newRegistered)
  }

  def deregister[K <: Key[V], V](cell: Cell[K, V]): Unit = {
    var success = false
    while (!success) {
      val registered = cellsNotDone.get()
      val newRegistered = registered.filterNot(_ == cell)
      success = cellsNotDone.compareAndSet(registered, newRegistered)
      //if (!success) throw new Exception("SHOULD NEVER HAPPEN")
      //success
    }
  }

  def quiescentIncompleteCells: Future[List[Cell[_, _]]] = {
    val p = Promise[List[Cell[_, _]]]
    this.onQuiescent { () =>
      val registered = this.cellsNotDone.get()
      p.success(registered)
    }
    p.future
  }

  def quiescentResolveCell: Future[Boolean] = {
    val p = Promise[Boolean]
    this.onQuiescent { () =>
      val registered = this.cellsNotDone.get()
      if (registered.nonEmpty) {
        registered.foreach { victimCell =>
          //val victimCell = registered.head
          victimCell.resolveCycle()
        }
      }
      p.success(true)
    }
    p.future
  }

  def execute(fun: () => Unit): Unit =
    execute(new Runnable { def run(): Unit = fun() })

  def execute(task: Runnable): Unit =
    pool.execute(task)

  def shutdown(): Unit = {
    // signal thread to terminate
    t.terminate = true
    t.join() // wait for thread to terminate
    pool.shutdown()
  }

  def reportFailure(t: Throwable): Unit =
    t.printStackTrace()
}
