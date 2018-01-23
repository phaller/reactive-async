package cell

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }
import lattice.Key
import org.opalj.graphs._

import scala.collection.immutable.Queue

/* Need to have reference equality for CAS.
 */
private class PoolState(val handlers: List[() => Unit] = List(), val submittedTasks: Int = 0) {
  def isQuiescent(): Boolean =
    submittedTasks == 0
}

class HandlerPool(parallelism: Int = 8, unhandledExceptionHandler: Throwable => Unit = _.printStackTrace()) {

  private val pool: ForkJoinPool = new ForkJoinPool(parallelism)

  private val poolState = new AtomicReference[PoolState](new PoolState)

  private val cellsNotDone = new AtomicReference[Map[Cell[_, _], Queue[SequentialCallbackRunnable[_, _]]]](Map()) // use `values` to store all pending sequential triggers

  @tailrec
  final def onQuiescent(handler: () => Unit): Unit = {
    val state = poolState.get()
    if (state.isQuiescent) {
      execute(new Runnable { def run(): Unit = handler() })
    } else {
      val newState = new PoolState(handler :: state.handlers, state.submittedTasks)
      val success = poolState.compareAndSet(state, newState)
      if (!success)
        onQuiescent(handler)
    }
  }

  def register[K <: Key[V], V](cell: Cell[K, V]): Unit = {
    val registered = cellsNotDone.get()
    val newRegistered = registered + (cell -> Queue())
    cellsNotDone.compareAndSet(registered, newRegistered)
  }

  def deregister[K <: Key[V], V](cell: Cell[K, V]): Unit = {
    var success = false
    while (!success) {
      val registered = cellsNotDone.get()
      val newRegistered = registered - cell
      success = cellsNotDone.compareAndSet(registered, newRegistered)
    }
  }

  def quiescentIncompleteCells: Future[List[Cell[_, _]]] = {
    val p = Promise[List[Cell[_, _]]]
    this.onQuiescent { () =>
      val registered = this.cellsNotDone.get()
      p.success(registered.keys.toList)
    }
    p.future
  }

  def whileQuiescentResolveCell[K <: Key[V], V]: Unit = {
    while (!cellsNotDone.get().isEmpty) {
      val fut = this.quiescentResolveCell
      Await.ready(fut, 15.minutes)
    }
  }

  def whileQuiescentResolveDefault[K <: Key[V], V]: Unit = {
    while (!cellsNotDone.get().isEmpty) {
      val fut = this.quiescentResolveDefaults
      Await.ready(fut, 15.minutes)
    }
  }

  def quiescentResolveCycles[K <: Key[V], V]: Future[Boolean] = {
    val p = Promise[Boolean]
    this.onQuiescent { () =>
      // Find one closed strongly connected component (cell)
      val registered: Seq[Cell[K, V]] = this.cellsNotDone.get().keys.asInstanceOf[Iterable[Cell[K, V]]].toSeq
      if (registered.nonEmpty) {
        val cSCCs = closedSCCs(registered, (cell: Cell[K, V]) => cell.totalCellDependencies)
        cSCCs.foreach(cSCC => resolveCycle(cSCC.asInstanceOf[Seq[Cell[K, V]]]))
      }
      p.success(true)
    }
    p.future
  }

  def quiescentResolveDefaults[K <: Key[V], V]: Future[Boolean] = {
    val p = Promise[Boolean]
    this.onQuiescent { () =>
      // Finds the rest of the unresolved cells
      val rest = this.cellsNotDone.get().keys.asInstanceOf[Iterable[Cell[K, V]]].toSeq
      if (rest.nonEmpty) {
        resolveDefault(rest)
      }
      p.success(true)
    }
    p.future
  }

  def quiescentResolveCell[K <: Key[V], V]: Future[Boolean] = {
    val p = Promise[Boolean]
    this.onQuiescent { () =>
      // Find one closed strongly connected component (cell)
      val registered: Seq[Cell[K, V]] = this.cellsNotDone.get().keys.asInstanceOf[Iterable[Cell[K, V]]].toSeq
      if (registered.nonEmpty) {
        val cSCCs = closedSCCs(registered, (cell: Cell[K, V]) => cell.totalCellDependencies)
        cSCCs.foreach(cSCC => resolveCycle(cSCC.asInstanceOf[Seq[Cell[K, V]]]))
      }
      // Finds the rest of the unresolved cells
      val rest = this.cellsNotDone.get().keys.asInstanceOf[Iterable[Cell[K, V]]].toSeq
      if (rest.nonEmpty) {
        resolveDefault(rest)
      }
      p.success(true)
    }
    p.future
  }

  /**
   * Resolves a cycle of unfinished cells.
   */
  private def resolveCycle[K <: Key[V], V](cells: Seq[Cell[K, V]]): Unit = {
    val key = cells.head.key
    val result = key.resolve(cells)

    for ((c, v) <- result) {
      cells.foreach(c.removeNextCallbacks(_))
      c.resolveWithValue(v)
    }
  }

  /**
   * Resolves a cell with default value.
   */
  private def resolveDefault[K <: Key[V], V](cells: Seq[Cell[K, V]]): Unit = {
    val key = cells.head.key
    val result = key.fallback(cells)

    for ((c, v) <- result) {
      cells.foreach(c.removeNextCallbacks(_))
      c.resolveWithValue(v)
    }
  }

  /**
   * Increase the number of submitted tasks.
   * Change the PoolState accordingly.
   */
  private def incSubmittedTasks(): Unit = {
    var submitSuccess = false
    while (!submitSuccess) {
      val state = poolState.get()
      val newState = new PoolState(state.handlers, state.submittedTasks + 1)
      submitSuccess = poolState.compareAndSet(state, newState)
    }
  }

  /**
   * Decrease the number of submitted tasks and run registered handlers, if quiescent.
   * Change the PoolState accordingly.
   */
  private def decSubmittedTasks(): Unit = {
    var success = false
    var handlersToRun: Option[List[() => Unit]] = None
    while (!success) {
      val state = poolState.get()
      if (state.submittedTasks > 1) {
        handlersToRun = None
        val newState = new PoolState(state.handlers, state.submittedTasks - 1)
        success = poolState.compareAndSet(state, newState)
      } else if (state.submittedTasks == 1) {
        handlersToRun = Some(state.handlers)
        val newState = new PoolState()
        success = poolState.compareAndSet(state, newState)
      } else {
        throw new Exception("BOOM")
      }
    }
    if (handlersToRun.nonEmpty) {
      handlersToRun.get.foreach { handler =>
        execute(new Runnable {
          def run(): Unit = handler()
        })
      }
    }
  }

  // Shouldn't we use:
  //def execute(f : => Unit) : Unit =
  //  execute(new Runnable{def run() : Unit = f})

  def execute(fun: () => Unit): Unit =
    execute(new Runnable { def run(): Unit = fun() })

  def execute(task: Runnable): Unit = {
    // Submit task to the pool
    incSubmittedTasks()

    // Run the task
    pool.execute(new Runnable {
      def run(): Unit = {
        try {
          task.run()
        } catch {
          case NonFatal(e) =>
            unhandledExceptionHandler(e)
        } finally {
          decSubmittedTasks()
        }
      }
    })
  }

  /**
   * Adds sequential callback.
   * The dependent cell is read from the NextDepRunnable object.
   *
   * @param callback The callback that should be run sequentially to all other sequential callbacks for the dependent cell.
   */
  private[cell] def scheduleSequentialCallback[K <: Key[V], V](callback: SequentialCallbackRunnable[K, V]): Unit = {
    incSubmittedTasks() // note that decSubmitted Tasks is called in callSequentialCallback

    val dependentCell = callback.dependentCell
    var success = false
    var startCallback = false
    while (!success) {
      val registered = cellsNotDone.get()
      if (registered.contains(dependentCell)) {
        val oldCallbackQueue = registered(dependentCell)
        val newCallbackQueue = oldCallbackQueue.enqueue(callback)
        val newRegistered = registered + (dependentCell -> newCallbackQueue)
        success = cellsNotDone.compareAndSet(registered, newRegistered)
        startCallback = oldCallbackQueue.isEmpty
      } else {
        success = true
      }
    }

    // If the list has been empty, then start execution the scheduled tasks. (Otherwise, some task is already running
    // and the newly added task will eventually run.
    if (startCallback)
      callSequentialCallback(dependentCell)
  }

  /**
   * Returns the the queue of yet to be run callbacks.
   * Called by callSequentialCallback after one callback has been run.
   * If the returned list is not empty, a next callback must be run.
   */
  @tailrec
  private def dequeueSequentialCallback[K <: Key[V], V](cell: Cell[K, V]): Queue[SequentialCallbackRunnable[_, _]] = {
    val registered = cellsNotDone.get()
    if (registered.contains(cell)) {
      // remove the task that has just been finished
      val oldCallbackQueue = registered(cell)
      val (_, newCallbackQueue) = oldCallbackQueue.dequeue
      val newRegistered = registered + (cell -> newCallbackQueue)

      // store the new list of tasks
      if (cellsNotDone.compareAndSet(registered, newRegistered)) newCallbackQueue
      else dequeueSequentialCallback(cell) // try again
    } else {
      // cell has already been completed by now. No callbacks need to be run any more
      Queue.empty
    }
  }

  private def callSequentialCallback[K <: Key[V], V](dependentCell: Cell[K, V]): Unit = {
    pool.execute(() => {
      val registered = cellsNotDone.get()

      // only call the callback, if the cell has not been completed
      if (registered.contains(dependentCell)) {
        val tasks = registered(dependentCell)
        /*
          Pop an element from the queue only if it is completely done!
          That way, one can always start running sequential callbacks, if the list has been empty.
         */
        val task = tasks.head.asInstanceOf[NextDepRunnable[K, V]] // The queue must not be empty! Caller has to assert this.

        try {
          task.run()
        } catch {
          case NonFatal(e) =>
            unhandledExceptionHandler(e)
        } finally {
          decSubmittedTasks()

          // The task has been run. Remove it. If the new list is not empty, callSequentialCallback(cell)
          if (dequeueSequentialCallback(dependentCell).nonEmpty)
            callSequentialCallback(dependentCell)
        }
      }
    })
  }

  def shutdown(): Unit =
    pool.shutdown()

  def reportFailure(t: Throwable): Unit =
    t.printStackTrace()
}
