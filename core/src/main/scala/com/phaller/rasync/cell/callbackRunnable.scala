package com.phaller.rasync
package cell

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.OnCompleteRunnable
import scala.util.{ Failure, Success, Try }

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
private[rasync] trait CallbackRunnable[V] extends Runnable with OnCompleteRunnable {
  val dependentCompleter: CellCompleter[V]

  /** The cell that triggers the callback. */
  val otherCell: Cell[V]

  protected val sequential: Boolean

  /** The callback to be called. It retrieves an updated value of otherCell and returns an Outcome for dependentCompleter. */
  val callback: (Cell[V], Try[ValueOutcome[V]]) => Outcome[V]

  // AtomicBoolean does not offer getAndAccumulate()
  private val completed = new AtomicReference[Boolean](false)

  /** Call the callback and use update dependentCompleter according to the callback's result. */
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
    if (!dependentCompleter.cell.isComplete) {
      val dependerState = otherCell.getState()

      // (1) we do not call the callback any more, if it has been
      // called with a final value before
      // (2) for final value: "lock" this callback
      val hadBeenCompleted = completed.getAndAccumulate(dependerState._2, _ || _)
      if (!hadBeenCompleted)
        try {
          val propagatedValue: Try[ValueOutcome[V]] =
            dependerState match {
              case (Success(v), isFinal) =>
                Success(Outcome(v, isFinal))
              case (Failure(e), _) => Failure(e)
            }
          val depRemoved = // see below for depRemoved
            callback(otherCell, propagatedValue) match {
              case NextOutcome(v) =>
                dependentCompleter.putNext(v)
                false
              case FinalOutcome(v) =>
                dependentCompleter.putFinal(v)
                dependentCompleter.cell.removeDependeeCell(otherCell)
                true
              case FreezeOutcome =>
                dependentCompleter.freeze()
                dependentCompleter.cell.removeDependeeCell(otherCell)
                true
              case NoOutcome => false /* do nothing */
            }
          // if the dependency has not been removed yet,
          // we can remove it, if a FinalOutcome has been propagted
          // or a Failuare has been propagated, i.e. the dependee had been completed
          if (!depRemoved && propagatedValue.map(_.isInstanceOf[FinalOutcome[_]]).getOrElse(true)) {
            dependentCompleter.cell.removeDependeeCell(otherCell)
          }
        } catch {
          // An exception thrown in a callback is stored as  the final value for the depender
          case e: Exception =>
            dependentCompleter.putFailure(Failure(e))
        }

    }
  }
}

/**
 * Run a callback concurrently, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[rasync] class ConcurrentCallbackRunnable[V](override val dependentCompleter: CellCompleter[V], override val otherCell: Cell[V], override val callback: (Cell[V], Try[ValueOutcome[V]]) => Outcome[V]) extends CallbackRunnable[V] {
  override protected final val sequential: Boolean = false
}

/**
 * Run a callback sequentially (for a dependent cell), if a value in another cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[rasync] class SequentialCallbackRunnable[V](override val dependentCompleter: CellCompleter[V], override val otherCell: Cell[V], override val callback: (Cell[V], Try[ValueOutcome[V]]) => Outcome[V]) extends CallbackRunnable[V] {
  override protected final val sequential: Boolean = true
}
