package cell

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.ForkJoinPool


class HandlerPool(parallelism: Int = 8) {

  private val pool: ForkJoinPool = new ForkJoinPool(parallelism)

  private val quiescentHandlers = new AtomicReference[List[() => Unit]](List())

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
}
