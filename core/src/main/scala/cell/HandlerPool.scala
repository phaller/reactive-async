package cell

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import lattice.Key
import org.opalj.graphs._

import scala.collection.immutable.Queue
import scala.util.Success

/* Need to have reference equality for CAS.
 */
private class PoolState(val handlers: List[() => Unit] = List(), val submittedTasks: Int = 0) {
  def isQuiescent(): Boolean =
    submittedTasks == 0
}

class HandlerPool(parallelism: Int = 8, unhandledExceptionHandler: Throwable => Unit = _.printStackTrace()) {

  private val pool: ForkJoinPool = new ForkJoinPool(parallelism)

  private val poolState = new AtomicReference[PoolState](new PoolState)

  private val cellsNotDone = new AtomicReference[Map[Cell[_, _], Queue[NextDepRunnable[_, _]]]](Map()) // use `values` to store all pending sequential triggers

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

  // Shouldn't we use:
  //def execute(f : => Unit) : Unit =
  //  execute(new Runnable{def run() : Unit = f})

  def execute(fun: () => Unit): Unit =
    execute(new Runnable { def run(): Unit = fun() })

  def execute(task: Runnable): Unit = {
    // Submit task to the pool
    var submitSuccess = false
    while (!submitSuccess) {
      val state = poolState.get()
      val newState = new PoolState(state.handlers, state.submittedTasks + 1)
      submitSuccess = poolState.compareAndSet(state, newState)
    }

    // Run the task
    pool.execute(new Runnable {
      def run(): Unit = {
        try {
          task.run()
        } catch {
          case NonFatal(e) =>
            unhandledExceptionHandler(e)
        } finally {
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
      }
    })
  }

  private[cell] def addSequentialCallback[K <: Key[V], V](callback: NextDepRunnable[K, V]): Unit = {
    val dependentCell = callback.completer.cell
    var success = false
    var startCallback = false
    while (!success) {
      val registered = cellsNotDone.get()
      if(registered.contains(dependentCell)) {
        val oldCallbackQueue = registered(dependentCell)
        val newCallbackQueue = oldCallbackQueue.enqueue(callback)
        val newRegistered = registered + (dependentCell -> newCallbackQueue)
        success = cellsNotDone.compareAndSet(registered, newRegistered)
        startCallback = oldCallbackQueue.nonEmpty
      } else {
        success = true
      }
    }

    // If the list has not been empty, then there is a tasks running that will end and then pick a new sequential callback to run.
    if(startCallback) callSequentialCallback(dependentCell)
  }

  /**
    Returns true, the queue is not empty after removing the first element.
   */
  private def dequeueSequentialCallback[K <: Key[V], V](cell: Cell[K, V]): Boolean = {
    var success = false
    var nonEmpty = false
    while (!success) {
      val registered = cellsNotDone.get()
      if(registered.contains(cell)) {
        val oldCallbackQueue = registered(cell)
        val (_, newCallbackQueue) = oldCallbackQueue.dequeue
        val newRegistered = registered + (cell -> newCallbackQueue)
        success = cellsNotDone.compareAndSet(registered, newRegistered)
        nonEmpty = newCallbackQueue.nonEmpty
      } else {
        success = true
      }
    }
    nonEmpty
  }

  private def callSequentialCallback[K <: Key[V], V](dependentCell: Cell[K, V]): Unit = {
    pool.execute(() => { // TODO Add a priority here, if the yet to be created PR gets accepted
      val registered = cellsNotDone.get()
      if (registered.contains(dependentCell)) {
        val tasks = registered(dependentCell)
        /*
          Pop an element from the queue only if it is completely done!
          That way, one can always run a callback, if the list has been empty.
         */
        val task = tasks.head.asInstanceOf[NextDepRunnable[K, V]] // The queue must not be empty! Caller has to assert this.
        task(Success(task.cell.getResult())) // TODO If #59 is rejected, explicitly pass the value to propagate around and use it here

        // The task has been run. Remove it.
        // If the new list is not empty, callSequentialCallback(cell)
        if (dequeueSequentialCallback(dependentCell))
          callSequentialCallback(dependentCell)

      }
    })
  }

  def shutdown(): Unit =
    pool.shutdown()

  def reportFailure(t: Throwable): Unit =
    t.printStackTrace()
}
