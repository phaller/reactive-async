package com.phaller.rasync
package pool

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicReference

import cell.{ Cell, CellCompleter, NoOutcome, Outcome }
import lattice.{ DefaultKey, Key, Updater }
import org.opalj.graphs._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success }
import scala.util.control.NonFatal

/* Need to have reference equality for CAS.
 */
private class PoolState(val handlers: List[() => Unit] = List(), val submittedTasks: Int = 0) {
  def isQuiescent(): Boolean =
    submittedTasks == 0
}

class HandlerPool[V](
  val key: Key[V] = new DefaultKey[V],
  val parallelism: Int = Runtime.getRuntime.availableProcessors(),
  unhandledExceptionHandler: Throwable => Unit = _.printStackTrace(),
  val schedulingStrategy: SchedulingStrategy = DefaultScheduling) {

  private val pool: AbstractExecutorService =
    if (schedulingStrategy == DefaultScheduling)
      new ForkJoinPool(parallelism)
    else
      new ThreadPoolExecutor(parallelism, parallelism, Int.MaxValue, TimeUnit.NANOSECONDS, new PriorityBlockingQueue[Runnable]())

  private val poolState = new AtomicReference[PoolState](new PoolState)

  private val cellsNotDone = new ConcurrentLinkedQueue[Cell[_]]()

  abstract class PriorityRunnable(val priority: Int) extends Runnable with Comparable[Runnable] {
    override def compareTo(t: Runnable): Int = {
      val p = t match {
        case runnable: PriorityRunnable => runnable.priority
        case _ => 1
      }
      priority - p
    }
  }

  /**
   * Returns a new cell in this HandlerPool.
   *
   * Creates a new cell with the given key. The `init` method is used to
   * determine an initial value for that cell and to set up dependencies via `whenNext`.
   * It gets called, when the cell is awaited, either directly by the triggerExecution method
   * of the HandlerPool or if a cell that depends on this cell is awaited.
   *
   * @param init A callback to return the initial value for this cell and to set up dependencies.
   * @param updater The updater used to update the value of this cell.
   * @return Returns a cell.
   */
  def mkCell(init: (Cell[V]) => Outcome[V])(implicit updater: Updater[V]): Cell[V] = {
    CellCompleter(init)(updater, this).cell
  }
  def mkSequentialCell(init: (Cell[V]) => Outcome[V])(implicit updater: Updater[V]): Cell[V] = {
    CellCompleter(init, sequential = true)(updater, this).cell
  }

  /**
   * Returns a new cell in this HandlerPool.
   *
   * Creates a new, completed cell with value `v`.
   *
   * @param updater The updater used to update the value of this cell.
   * @return Returns a cell with value `v`.
   */
  def mkCompletedCell(result: V)(implicit updater: Updater[V]): Cell[V] = {
    CellCompleter.completed(result)(updater, this).cell
  }

  /**
   * Add an event handler that is called, when the pool reaches quiescence.
   *
   * The `handler` is called once after the pool reaches quiescence the first time
   * after it has been added.
   */
  @tailrec
  final def onQuiescent(handler: () => Unit): Unit = {
    val state = poolState.get()
    if (state.isQuiescent) {
      execute(new Runnable { def run(): Unit = handler() }, 0)
    } else {
      val newState = new PoolState(handler :: state.handlers, state.submittedTasks)
      val success = poolState.compareAndSet(state, newState)
      if (!success)
        onQuiescent(handler)
    }
  }

  /**
   * Register a cell with this HandlerPool.
   *
   * @param cell The cell.
   */
  def register(cell: Cell[V]): Unit =
    cellsNotDone.add(cell)

  /**
   * Deregister a cell from this HandlerPool.
   *
   * @param cell The cell.
   */
  def deregister(cell: Cell[V]): Unit =
    cellsNotDone.remove(cell)

  /**
   * Remove all completed cells from cellsNotDone. Cells are not removed on deregister, but when the queue is
   * queried.
   */
  private def deregisterCompletedCells(): Unit = {
    cellsNotDone.removeIf(_.isComplete)
  }

  /** Returns all non-completed cells, when quiescence is reached. */
  def quiescentIncompleteCells: Future[Iterable[Cell[_]]] = {
    val p = Promise[Iterable[Cell[_]]]
    this.onQuiescent { () =>
      deregisterCompletedCells()
      p.success(cellsNotDone.asScala)
    }
    p.future
  }

  /**
   * Wait for a quiescent state.
   * Afterwards, resolve all cells without dependencies with the respective
   * `fallback` value calculated by it's `Key`.
   * Also, resolve cycles without dependencies (cSCCs) using the respective `Key`'s `resolve` method.
   * Both might lead to computations on other cells being triggered.
   * If more cells are unresolved, recursively wait for resolution.
   *
   * @return The future is set once the resolve is finished and the quiescent state is reached.
   *         The boolean parameter indicates if cycles have been resolved or not.
   */
  def quiescentResolveCell: Future[Unit] = {
    val p = Promise[Unit]
    this.onQuiescent { () =>
      deregisterCompletedCells()

      val activeCells = this.cellsNotDone.asScala
        .filter(_.tasksActive())
        .asInstanceOf[Iterable[Cell[V]]]

      val independent = activeCells.filter(_.isIndependent())
      val waitAgain: Boolean =
        if (independent.nonEmpty) {
          // Resolve independent cells with fallback values
          resolveIndependent(independent)
        } else {
          // Otherwise, find and resolve closed strongly connected components and resolve them.

          // Find closed strongly connected component (cell)
          if (activeCells.isEmpty) {
            false
          } else {
            val cSCCs = closedSCCs[Cell[V]](activeCells, (cell: Cell[V]) => cell.cellDependencies)
            cSCCs
              .map(resolveCycle) // resolve all cSCCs
              .exists(b => b) // return if any resolution took place
          }
        }

      // Wait again for quiescent state. It's possible that other tasks where scheduled while
      // resolving the cells.
      if (waitAgain) {
        p.completeWith(quiescentResolveCell)
      } else {
        p.success(())
      }
    }
    p.future
  }

  /**
   * Resolves a cycle of unfinished cells via the key's `resolve` method.
   */
  private def resolveCycle(cells: Iterable[Cell[V]]): Boolean =
    resolve(cells, key.resolve)

  /**
   * Resolves a cell with default value with the key's `fallback` method.
   */
  private def resolveIndependent(cells: Iterable[Cell[V]]): Boolean =
    resolve(cells, key.fallback)

  /** Resolve all cells with the associated value. */
  private def resolve(cells: Iterable[Cell[V]], k: (Iterable[Cell[V]]) => Iterable[(Cell[V], V)]): Boolean = {
    try {
      val results = k(cells)
      val dontCall = results.map(_._1).toSeq
      for ((c, v) <- results) {
        val res = Success(v)
        execute(new Runnable {
          override def run(): Unit = {
            // resolve each cell with the given value
            // but do not propagate among the cells in the same set (i.e. the same cSCC)
            c.resolveWithValue(res, dontCall)
          }
        }, schedulingStrategy.calcPriority(c, res))
      }
      results.nonEmpty
    } catch {
      case e: Exception =>
        // if an exception occurs, resolve all cells with a failure
        val f = Failure(e)
        val dontCall = cells.toSeq
        cells.foreach(c =>
          execute(() => c.resolveWithValue(f, dontCall), schedulingStrategy.calcPriority(c, f)))
        cells.nonEmpty
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
  private def decSubmittedTasks(i: Int = 1): Unit = {
    var success = false
    var handlersToRun: Option[List[() => Unit]] = None

    while (!success) { // reapeat until compareAndSet succeeded
      val state = poolState.get()
      if (state.submittedTasks > i) {
        // we can simply decrease the counter
        handlersToRun = None
        val newState = new PoolState(state.handlers, state.submittedTasks - i)
        success = poolState.compareAndSet(state, newState)
      } else if (state.submittedTasks == i) {
        // counter will drop to zero, so we need to call quiescent handlers later
        handlersToRun = Some(state.handlers)
        // a fresh state without any quiescent handler attached – those get called at most once!
        val newState = new PoolState()
        success = poolState.compareAndSet(state, newState)
      } else {
        throw new Exception("BOOM")
      }
    }
    // run all handler that have been attached at the time quiescence was reached
    if (handlersToRun.nonEmpty) {
      handlersToRun.get.foreach { handler =>
        execute(new Runnable {
          def run(): Unit = handler()
        }, 0) // TODO set a priority
      }
    }
  }

  // Shouldn't we use:
  //def execute(f : => Unit) : Unit =
  //  execute(new Runnable{def run() : Unit = f})

  def execute(fun: () => Unit, priority: Int): Unit =
    execute(new Runnable { def run(): Unit = fun() }, priority)

  def execute(task: Runnable, priority: Int = 0): Unit = {
    // Submit task to the pool
    incSubmittedTasks()

    // Run the task
    try {
      pool.execute(new PriorityRunnable(priority) {
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
    } catch {
      // If pool.execute() failed, we need to count down now.
      // (Normally, decSubmittedTasks is called after task.run())
      case e: Exception =>
        decSubmittedTasks()
        throw e
    }
  }

  /**
   * If a cell is triggered, it's `init` method is
   * run to both get an initial (or possibly final) value
   * and to set up dependencies (via whenNext/whenComplete).
   * All dependees automatically get triggered.
   *
   * @param cell The cell that is triggered.
   */
  private[rasync] def triggerExecution(cell: Cell[V], priority: Int = 0): Unit = {
    if (cell.setTasksActive())
      // if the cell's state has successfully been changed, schedule further computations
      execute(() => {
        val completer = cell.completer
        try {
          val outcome = completer.init(cell) // call the init method
          outcome match {
            case Outcome(v, isFinal) => completer.put(v, isFinal)
            case NoOutcome => /* don't do anything */
          }
        } catch {
          case e: Exception => completer.putFailure(Failure(e))
        }
      }, priority)
  }

  /**
   * Possibly initiates an orderly shutdown in which previously
   * submitted tasks are executed, but no new tasks are accepted.
   * This method should only be called, when the pool is quiescent.
   */
  def shutdown(): Unit =
    pool.shutdown()

  /**
   * Waits for quiescence, then shuts the pool down.
   */
  def onQuiescenceShutdown(): Unit =
    this.onQuiescent(() => pool.shutdown())

  def reportFailure(t: Throwable): Unit =
    t.printStackTrace()

}
