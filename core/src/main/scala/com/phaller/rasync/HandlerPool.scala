package com.phaller.rasync

import java.util.concurrent.{ ConcurrentLinkedQueue, ForkJoinPool }
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._
import lattice.{ DefaultKey, Key, Updater }
import org.opalj.graphs._
import scala.collection.JavaConverters._

/* Need to have reference equality for CAS.
 */
private class PoolState(val handlers: List[() => Unit] = List(), val submittedTasks: Int = 0) {
  def isQuiescent(): Boolean =
    submittedTasks == 0
}

class HandlerPool(val parallelism: Int = 8, unhandledExceptionHandler: Throwable => Unit = _.printStackTrace()) {

  private val pool: ForkJoinPool = new ForkJoinPool(parallelism)

  private val poolState = new AtomicReference[PoolState](new PoolState)

  private val cellsNotDone = new ConcurrentLinkedQueue[Cell[_, _]]()

  /**
   * Returns a new cell in this HandlerPool.
   *
   * Creates a new cell with the given key. The `init` method is used to
   * determine an initial value for that cell and to set up dependencies via `whenNext`.
   * It gets called, when the cell is awaited, either directly by the triggerExecution method
   * of the HandlerPool or if a cell that depends on this cell is awaited.
   *
   * @param key The key to resolve this cell if in a cycle or no result computed.
   * @param init A callback to return the initial value for this cell and to set up dependencies.
   * @param updater The updater used to update the value of this cell.
   * @return Returns a cell.
   */
  def mkCell[K <: Key[V], V](key: K, init: (Cell[K, V]) => Outcome[V])(implicit updater: Updater[V]): Cell[K, V] = {
    CellCompleter(key, init)(updater, this).cell
  }

  /**
   * Returns a new cell in this HandlerPool.
   *
   * Creates a new, completed cell with value `v`.
   *
   * @param updater The updater used to update the value of this cell.
   * @return Returns a cell with value `v`.
   */
  def mkCompletedCell[V](result: V)(implicit updater: Updater[V]): Cell[DefaultKey[V], V] = {
    CellCompleter.completed(result)(updater, this).cell
  }

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

  /**
   * Register a cell with this HandlerPool.
   *
   * @param cell The cell.
   */
  def register[K <: Key[V], V](cell: Cell[K, V]): Unit =
    cellsNotDone.add(cell)

  /**
   * Deregister a cell from this HandlerPool.
   *
   * @param cell The cell.
   */
  def deregister[K <: Key[V], V](cell: Cell[K, V]): Unit =
    cellsNotDone.remove(cell)

  /**
   * Remove all completed cells from cellsNotDone. Cells are not removed on deregister, but when the queue is
   * queried.
   */
  private def deregisterCompletedCells(): Unit = {
    cellsNotDone.removeIf(_.isComplete)
  }

  /** Returns all non-completed cells, when quiescence is reached. */
  def quiescentIncompleteCells: Future[Iterable[Cell[_, _]]] = {
    val p = Promise[Iterable[Cell[_, _]]]
    this.onQuiescent { () =>
      deregisterCompletedCells()
      p.success(cellsNotDone.asScala)
    }
    p.future
  }

  def whileQuiescentResolveCell[K <: Key[V], V]: Unit = {
    while (!cellsNotDone.isEmpty) {
      val fut = this.quiescentResolveCell
      Await.ready(fut, 15.minutes)
    }
  }

  def whileQuiescentResolveDefault[K <: Key[V], V]: Unit = {
    while (!cellsNotDone.isEmpty) {
      val fut = this.quiescentResolveDefaults
      Await.ready(fut, 15.minutes)
    }
  }

  /**
   * Wait for a quiescent state when no more tasks are being executed. Afterwards, it will resolve
   * unfinished cycles (cSCCs) of cells using the keys resolve function and recursively wait for resolution.
   *
   * @return The future will be set once the resolve is finished and the quiescent state is reached.
   *         The boolean parameter indicates if cycles where resolved or not.
   */
  def quiescentResolveCycles[K <: Key[V], V]: Future[Boolean] = {
    val p = Promise[Boolean]
    this.onQuiescent { () =>
      // Find one closed strongly connected component (cell)
      deregisterCompletedCells()
      val registered: Seq[Cell[K, V]] = this.cellsNotDone.asScala.filter(_.tasksActive()).asInstanceOf[Iterable[Cell[K, V]]].toSeq
      if (registered.nonEmpty) {
        val cSCCs = closedSCCs(registered, (cell: Cell[K, V]) => cell.totalCellDependencies)
        cSCCs.foreach(cSCC => resolveCycle(cSCC.asInstanceOf[Seq[Cell[K, V]]]))

        // Wait again for quiescent state. It's possible that other tasks where scheduled while
        // resolving the cells.
        if (cSCCs.nonEmpty) {
          p.completeWith(quiescentResolveCycles)
        } else {
          p.success(false)
        }
      } else {
        p.success(false)
      }
    }
    p.future
  }

  /**
   * Wait for a quiescent state when no more tasks are being executed. Afterwards, it will resolve
   * unfinished cells using the keys fallback function and recursively wait for resolution.
   *
   * Note that this also resolves cells that have dependencies on other cells – in contrast to
   * quiescentResolveCell()
   *
   * @return The future will be set once the resolve is finished and the quiescent state is reached.
   *         The boolean parameter indicates if cycles where resolved or not.
   */
  def quiescentResolveDefaults[K <: Key[V], V]: Future[Boolean] = {
    val p = Promise[Boolean]
    this.onQuiescent { () =>
      // Finds the rest of the unresolved cells (that have been triggered)
      deregisterCompletedCells()
      val rest = this.cellsNotDone.asScala
        .filter(_.tasksActive())
        .asInstanceOf[Iterable[Cell[K, V]]].toSeq
      if (rest.nonEmpty) {
        resolveDefault(rest)

        // Wait again for quiescent state. It's possible that other tasks where scheduled while
        // resolving the cells.
        p.completeWith(quiescentResolveDefaults)
      } else {
        p.success(false)
      }
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
  def quiescentResolveCell[K <: Key[V], V]: Future[Boolean] = {
    val p = Promise[Boolean]
    this.onQuiescent { () =>
      deregisterCompletedCells()
      val activeCells = this.cellsNotDone.asScala.filter(_.tasksActive()).asInstanceOf[Iterable[Cell[K, V]]].toSeq
      var resolvedCycles = false

      val independent = activeCells.filter(_.isIndependent())
      if (independent.nonEmpty) {
        // Resolve independent cells with fallback values
        resolveDefault(independent)
      } else {
        // Otherwise, find and resolve closed strongly connected components and resolve them.

        // Find closed strongly connected component (cell)
        if (activeCells.nonEmpty) {
          val cSCCs = closedSCCs(activeCells, (cell: Cell[K, V]) => cell.totalCellDependencies)
          cSCCs.foreach(cSCC => resolveCycle(cSCC.asInstanceOf[Seq[Cell[K, V]]]))
          resolvedCycles = cSCCs.nonEmpty
        }
      }

      // Wait again for quiescent state. It's possible that other tasks where scheduled while
      // resolving the cells.
      if (resolvedCycles || independent.nonEmpty) {
        p.completeWith(quiescentResolveCell)
      } else {
        p.success(false)
      }
    }
    p.future
  }

  /**
   * Resolves a cycle of unfinished cells via the key's `resolve` method.
   */
  private def resolveCycle[K <: Key[V], V](cells: Seq[Cell[K, V]]): Unit =
    resolve(cells.head.key.resolve(cells))

  /**
   * Resolves a cell with default value with the key's `fallback` method.
   */
  private def resolveDefault[K <: Key[V], V](cells: Seq[Cell[K, V]]): Unit =
    resolve(cells.head.key.fallback(cells))

  /** Resolve all cells with the associated value. */
  private def resolve[K <: Key[V], V](results: Seq[(Cell[K, V], V)]): Unit = {
    for ((c, v) <- results)
      execute(new Runnable {
        override def run(): Unit = {
          // Remove all callbacks that target other cells of this set.
          // The result of those cells is explicitely given in `results`.
          c.removeAllCallbacks(results.map(_._1))
          // we can now safely put a final value
          c.resolveWithValue(v)
        }
      })
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
    while (!success) {
      val state = poolState.get()
      if (state.submittedTasks > i) {
        handlersToRun = None
        val newState = new PoolState(state.handlers, state.submittedTasks - i)
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
   * If a cell is triggered, it's `init` method is
   * run to both get an initial (or possibly final) value
   * and to set up dependencies (via whenNext/whenComplete).
   * All dependees automatically get triggered.
   *
   * @param cell The cell that is triggered.
   */
  private[rasync] def triggerExecution[K <: Key[V], V](cell: Cell[K, V]): Unit = {
    if (cell.setTasksActive())
      execute(() => {
        val completer = cell.completer
        val outcome = completer.init(cell)
        outcome match {
          case Outcome(v, isFinal) => completer.put(v, isFinal)
          case NoOutcome => /* don't do anything */
        }
      })
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
