package com.phaller.rasync
package cell

import java.util.concurrent.atomic.AtomicReference

import pool.HandlerPool
import scala.annotation.tailrec

import scala.concurrent.OnCompleteRunnable
import scala.util.{ Failure, Success, Try }

import com.phaller.rasync.util.Counter

/**
 * CallbackRunnables are tasks that need to be run, when a value of a cell changes, that
 * some completer depends on.
 *
 * CallbackRunnables store information about the involved cells and the callback to
 * be run.
 */
private[rasync] abstract class CallbackRunnable[V, E >: Null] extends Runnable with OnCompleteRunnable {
  protected val pool: HandlerPool[V, E]

  protected val dependentCompleter: CellCompleter[V, E]

  /** The callback to be called. It retrieves an updated value of otherCell and returns an Outcome for dependentCompleter. */
  protected val callback: Iterable[(Cell[V, E], Try[ValueOutcome[V]])] ⇒ Outcome[V]

  protected val updatedDependees = new AtomicReference[Set[Cell[V, E]]](Set.empty)
  protected var prio: Int = Int.MaxValue

  @tailrec
  final def addUpdate(other: Cell[V, E]): Unit = {
    val oldUpdatedDependees = updatedDependees.get
    val newUpdatedDependees = oldUpdatedDependees + other
    if (updatedDependees.compareAndSet(oldUpdatedDependees, newUpdatedDependees)) {

      // We store a priority for sequential execution of this callbackRunnable.
      // It is set to the highest priority found among the dependees that are
      // part of this update.
      // This computation of prio is not thread-safe but this does not matter for
      // priorities are no hard requirement anyway.
      prio = Math.min(prio, pool.schedulingStrategy.calcPriority(dependentCompleter.cell, other, other.getState()))

      // The first incoming update (since the last execution) starts this runnable.
      // Other cells might still be added to updatedDependees concurrently, the runnable
      // will collect all updates and forward them altogether.
      if (oldUpdatedDependees.isEmpty) {
        pool.execute(this, prio)
        Counter.inc("CallbackRunnable.addUpdate.triggerExecution")
      } else {
        Counter.inc("CallbackRunnable.addUpdate.aggregations")
      }
    } else addUpdate(other) // retry
  }

  /**
   * Call the callback and update dependentCompleter according to the callback's result.
   * This method is implemented by `ConcurrentCallbackRunnable` and `SequentialCalllbackRunnable`,
   * where the latter implementation ensures that the callback is run sequentially.
   */
  override def run(): Unit

  protected def callCallback(): Unit = {
    if (!dependentCompleter.cell.isComplete) {

      try {
        // Remove all updates from the list of updates that need to be handled – they will now be handled
        val dependees = updatedDependees.getAndSet(Set.empty)
        val propagations = dependees.iterator.map(c ⇒ (c, c.getState())).toIterable

        val depsRemoved = // see below for depsRemoved
          callback(propagations) match {
            case NextOutcome(v) ⇒
              dependentCompleter.putNext(v)
              false
            case FinalOutcome(v) ⇒
              dependentCompleter.putFinal(v)
              true
            case FreezeOutcome ⇒
              dependentCompleter.freeze()
              true
            case NoOutcome ⇒
              // Do not change the value of the cell
              // but remove all dependees that have had
              // a final value from the lists of dependees.
              false
          }
        // if the dependency has not been removed yet,
        // we can remove it, if a FinalOutcome has been propagted
        // or a Failuare has been propagated, i.e. the dependee had been completed
        // and cannot change later
        if (!depsRemoved) {
          val toRemove = propagations.iterator.filter({
            case (_, Success(NextOutcome(_))) ⇒ false
            case _ ⇒ true
          }).map(_._1).toIterable
          dependentCompleter.cell.removeDependeeCells(toRemove)
        }
      } catch {
        // An exception thrown in a callback is stored as the final value for the depender
        case e: Exception ⇒
          dependentCompleter.putFailure(Failure(e))
      }
    }
  }
}

/**
 * Run a callback concurrently, if a value in a cell changes.
 */
private[rasync] class ConcurrentCallbackRunnable[V, E >: Null](override val pool: HandlerPool[V, E], override val dependentCompleter: CellCompleter[V, E], override val callback: Iterable[(Cell[V, E], Try[ValueOutcome[V]])] ⇒ Outcome[V]) extends CallbackRunnable[V, E] {
  override def run(): Unit =
    callCallback()
}

/**
 * Run a callback sequentially (for a dependent cell), if a value in another cell changes.
 */
private[rasync] class SequentialCallbackRunnable[V, E >: Null](override val pool: HandlerPool[V, E], override val dependentCompleter: CellCompleter[V, E], override val callback: Iterable[(Cell[V, E], Try[ValueOutcome[V]])] ⇒ Outcome[V]) extends CallbackRunnable[V, E] {
  override def run(): Unit =
    dependentCompleter.sequential(callCallback _, prio)
}
