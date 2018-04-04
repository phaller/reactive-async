package com.phaller.rasync

import java.util.concurrent.atomic.AtomicBoolean

import lattice.Key

import scala.concurrent.OnCompleteRunnable
import scala.util.control.NonFatal

/**
 * Run a callback in a handler pool, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[rasync] trait CallbackRunnable[K <: Key[V], V] extends Runnable with OnCompleteRunnable {
  /** The handler pool that runs the callback function. */
  val pool: HandlerPool

  /** The cell that awaits this callback. */
  val dependentCompleter: CellCompleter[K, V]

  /** The cell that triggers the callback. */
  val otherCell: Cell[K, V]

  protected val completeDep: Boolean
  protected val sequential: Boolean

  /** The callback to be called. It retrieves an updated value of otherCell and returns an Outcome for dependentCompleter. */
  val callback: Any // TODO Is there a better supertype for (a) (V, Bool)=>Outcome[V] and (b) V=>Outcome[V]. Nothing=>Outcome[V] does not work.

  /** Add this CallbackRunnable to its handler pool. */
  def execute(): Unit =
    if (otherCell.peekFor(dependentCompleter.cell, completeDep) != NoOutcome) // TODO For COMPLETECallbackRunnables, one could use != FinalOutcome
      try pool.execute(this)
      catch { case NonFatal(t) => pool reportFailure t }

  /** Call the callback and use its result in dependentCompleter. */
  override def run(): Unit
}

/**
 * Run a callback concurrently, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[rasync] trait ConcurrentCallbackRunnable[K <: Key[V], V] extends CallbackRunnable[K, V] {
  override protected final val sequential: Boolean = false
}

/**
 * Run a callback sequentially (for a dependent cell), if a value in another cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[rasync] trait SequentialCallbackRunnable[K <: Key[V], V] extends CallbackRunnable[K, V] {
  override protected final val sequential: Boolean = true
}

/**
 * To be run when `otherCell` gets its final update.
 * @param pool          The handler pool that runs the callback function
 * @param dependentCompleter The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[rasync] abstract class CompleteCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V], // needed to not call whenNext callback, if whenComplete callback exists.
  override val otherCell: Cell[K, V],
  override val callback: V => Outcome[V])
  extends CallbackRunnable[K, V] {

  override protected final val completeDep = true
  // must be filled in before running it
  var started: AtomicBoolean = new AtomicBoolean(false)

  protected def callCallback(x: V): Outcome[V] = {
    if (sequential) {
      dependentCompleter.sequential {
        callback(x)
      }
    } else {
      callback(x)
    }
  }

  def run(): Unit = {
    if (!started.getAndSet(true)) { // can't complete it twice
      otherCell.dequeueFor(dependentCompleter.cell, completeDep) match {
        case FinalOutcome(x) =>
          callCallback(x) match {
            case FinalOutcome(v) =>
              dependentCompleter.putFinal(v) // callbacks will be removed by putFinal()
            case NextOutcome(v) =>
              dependentCompleter.putNext(v)
              dependentCompleter.cell.removeCompleteCallbacks(otherCell)
            case NoOutcome =>
              dependentCompleter.cell.removeCompleteCallbacks(otherCell)
          }
        case _ => /* This is a whenCompleteDependency. Ignore any non-final values of otherCell. */
      }
    }
  }
}

private[rasync] class CompleteConcurrentCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCompleter: CellCompleter[K, V], override val otherCell: Cell[K, V], override val callback: V => Outcome[V])
  extends CompleteCallbackRunnable[K, V](pool, dependentCompleter, otherCell, callback)
  with ConcurrentCallbackRunnable[K, V]

private[rasync] class CompleteSequentialCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCompleter: CellCompleter[K, V], override val otherCell: Cell[K, V], override val callback: V => Outcome[V])
  extends CompleteCallbackRunnable[K, V](pool, dependentCompleter, otherCell, callback)
  with SequentialCallbackRunnable[K, V]

/* To be run when `otherCell` gets an update.
 * @param pool          The handler pool that runs the callback function
 * @param dependentCompleter The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[rasync] abstract class NextCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V], // needed to not call whenNext callback, if whenComplete callback exists.
  override val otherCell: Cell[K, V],
  override val callback: V => Outcome[V])
  extends CallbackRunnable[K, V] {

  override protected final val completeDep = false

  protected def callCallback(x: V): Outcome[V] = {
    // TODO fix synchronized
    if (sequential) {
      dependentCompleter.sequential {
        callback(x)
      }
    } else {
      callback(x)
    }
  }

  def run(): Unit = {
    otherCell.dequeueFor(dependentCompleter.cell, completeDep) match {
      case Outcome(x, isFinal) =>
        callCallback(x) match {
          case NextOutcome(v) =>
            dependentCompleter.putNext(v)
          case FinalOutcome(v) =>
            dependentCompleter.putFinal(v)
          case _ => /* do nothing, the value of */
        }

        // If `otherCell` has been completed with `x`, we do not depent on it any more.
        if (isFinal) dependentCompleter.cell.removeNextCallbacks(otherCell)
      case _ => /* No new value is present. */
    }
  }
}

private[rasync] class NextConcurrentCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCompleter: CellCompleter[K, V], override val otherCell: Cell[K, V], override val callback: V => Outcome[V])
  extends NextCallbackRunnable[K, V](pool, dependentCompleter, otherCell, callback) with ConcurrentCallbackRunnable[K, V]

private[rasync] class NextSequentialCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCompleter: CellCompleter[K, V], override val otherCell: Cell[K, V], override val callback: V => Outcome[V])
  extends NextCallbackRunnable[K, V](pool, dependentCompleter, otherCell, callback) with SequentialCallbackRunnable[K, V]

/* To be run when `otherCell` gets an update.
* @param pool          The handler pool that runs the callback function
* @param dependentCompleter The cell, that depends on `otherCell`.
* @param otherCell     Cell that triggers this callback.
* @param callback      Callback function that is triggered on an onNext event
*/
private[rasync] abstract class CombinedCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V], // needed to not call whenNext callback, if whenComplete callback exists.
  override val otherCell: Cell[K, V],
  override val callback: (V, Boolean) => Outcome[V])
  extends CallbackRunnable[K, V] {

  override protected final val completeDep = false

  def run(): Unit = {
    if (sequential) {
      dependentCompleter.sequential {
        callCallback()
      }
    } else {
      callCallback()
    }
  }

  protected def callCallback(): Unit = {
    if (dependentCompleter.cell.isComplete) {
      return ;
    }
    otherCell.dequeueFor(dependentCompleter.cell, completeDep) match {
      case Outcome(x, isFinal) =>
        callback(x, isFinal) match {
          case NextOutcome(v) =>
            dependentCompleter.putNext(v)
          case FinalOutcome(v) =>
            dependentCompleter.putFinal(v)
          case _ => /* do nothing, the value of */
        }

        // If `otherCell` has been completed with `x`, we do not depent on it any more.
        if (isFinal) dependentCompleter.cell.removeCombinedCallbacks(otherCell)
      case _ => /* No new value is present. */
    }
  }
}

private[rasync] class CombinedConcurrentCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCompleter: CellCompleter[K, V], override val otherCell: Cell[K, V], override val callback: (V, Boolean) => Outcome[V])
  extends CombinedCallbackRunnable[K, V](pool, dependentCompleter, otherCell, callback) with ConcurrentCallbackRunnable[K, V]

private[rasync] class CombinedSequentialCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCompleter: CellCompleter[K, V], override val otherCell: Cell[K, V], override val callback: (V, Boolean) => Outcome[V])
  extends CombinedCallbackRunnable[K, V](pool, dependentCompleter, otherCell, callback) with SequentialCallbackRunnable[K, V]
