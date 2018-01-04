package com.phaller.rasync

import java.util.concurrent.atomic.AtomicBoolean

import lattice.Key

import scala.concurrent.OnCompleteRunnable
import scala.util.control.NonFatal

/**
 * CallbackRunnables are tasks that need to be run, when a value of a cell changes, that
 * some completer depends on.
 *
 * CallbackRunnables store information about whether the dependency has been registered via whenComplete/whenNext
 * and when/whenSequential and the involved cells/completers.
 *
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
  // This method is not needed currently, as all callbackRunnables for one update are run in the same thread.
  // Also, this is the only use of the peekFor method, so if tests show that the current implementation is
  // faster, both execute() and peekFor() can be removed.
  def execute(): Unit =
    if (otherCell.peekFor(dependentCompleter.cell, completeDep) != NoOutcome) // for COMPLETECallbackRunnables, one could use != FinalOutcome
      try pool.execute(this, pool.schedulingStrategy.calcPriority(dependentCompleter.cell, otherCell))
      catch { case NonFatal(t) => pool reportFailure t }

  /** Call the callback and use update dependentCompleter according to the callback's result. */
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
      if (dependentCompleter.cell.isComplete) {
        return ;
      }

      // poll the staged value from otherCell and remove it from the queue to not call the callback with the same argument twice.
      otherCell.pollFor(dependentCompleter.cell, completeDep) match {
        case FinalOutcome(x) =>
          callCallback(x) match { // execute callback
            case FinalOutcome(v) =>
              dependentCompleter.putFinal(v) // callbacks will be removed by putFinal()
            case NextOutcome(v) =>
              dependentCompleter.putNext(v)
              // We have read the final value of `otherCell`, so we do not depend on it any more
              dependentCompleter.cell.removeCompleteCallbacks(otherCell)
            case NoOutcome =>
              // We have read the final value of `otherCell`, so we do not depend on it any more
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
    if (sequential) {
      dependentCompleter.sequential {
        callback(x)
      }
    } else {
      callback(x)
    }
  }

  def run(): Unit = {
    if (dependentCompleter.cell.isComplete) {
      return ;
    }

    // poll the staged value from otherCell and remove it from the queue to not call the callback with the same argument twice.
    otherCell.pollFor(dependentCompleter.cell, completeDep) match {
      case Outcome(x, isFinal) =>
        callCallback(x) match {
          case NextOutcome(v) =>
            dependentCompleter.putNext(v)
          case FinalOutcome(v) =>
            dependentCompleter.putFinal(v)
          case _ => /* do nothing, the value of */
        }

        // If `otherCell` has been completed with `x`, the nextCallback can be removed
        // (completeCallbacks will be removed by themselves after they have been run)
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

    // poll the staged value from otherCell and remove it from the queue to not call the callback with the same argument twice.
    otherCell.pollFor(dependentCompleter.cell, completeDep) match {
      case Outcome(x, isFinal) =>
        callback(x, isFinal) match {
          case NextOutcome(v) =>
            dependentCompleter.putNext(v)
          case FinalOutcome(v) =>
            dependentCompleter.putFinal(v)
          case _ => /* do nothing, the value of */
        }

        // If `otherCell` has been completed with `x`, the nextCallback can be removed
        if (isFinal) dependentCompleter.cell.removeCombinedCallbacks(otherCell)
      case _ => /* No new value is present. */
    }
  }
}

private[rasync] class CombinedConcurrentCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCompleter: CellCompleter[K, V], override val otherCell: Cell[K, V], override val callback: (V, Boolean) => Outcome[V])
  extends CombinedCallbackRunnable[K, V](pool, dependentCompleter, otherCell, callback) with ConcurrentCallbackRunnable[K, V]

private[rasync] class CombinedSequentialCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCompleter: CellCompleter[K, V], override val otherCell: Cell[K, V], override val callback: (V, Boolean) => Outcome[V])
  extends CombinedCallbackRunnable[K, V](pool, dependentCompleter, otherCell, callback) with SequentialCallbackRunnable[K, V]
