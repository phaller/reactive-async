
import java.util.concurrent.atomic._
import java.util.concurrent.ExecutionException

import scala.annotation.tailrec

import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

import scala.concurrent.{ExecutionContext, OnCompleteRunnable}


/**
 * Example:
 *
 *   val barRetTypeCell: Cell[(Entity, PropertyKind), ObjectType]
 */
trait Cell[K, V] {
  def key: K
  def property: V

  def dependencies: Seq[K]
  def addDependency(other: K)
  def removeDependency(other: K)

  // sobald sich der Wert dieser Cell ändert, müssen die dependee Cells benachrichtigt werden
  def dependees: Seq[K]
  def addDependee(k: K): Unit
  def removeDependee(k: K): Unit

  /**
   * Adds a dependency on some `other` cell.
   *
   * Example:
   *   whenComplete(cell, x => !x, Impure) // if `cell` is completed and the predicate is true (meaning
   *                                       // `cell` is impure), `this` cell can be completed with constant `Impure`
   *
   * @param other  Cell that `this` Cell depends on.
   * @param pred   Predicate used to decide whether a final result of `this` Cell can be computed early.
   *               `pred` is applied to value of `other` cell.
   * @param value  Early result value.
   */
  def whenComplete(other: Cell[K, V], pred: V => Boolean, value: V): Unit

  /**
   * Registers a call-back function to be invoked when quiescence is reached, but `this` cell has not been
   * completed, yet. The call-back function is passed a sequence of the cells that `this` cell depends on.
   */
  def onCycle(callback: Seq[Cell[K, V]] => V)

  // internal API

  // Schedules execution of `callback` when next intermediate result is available.
  def onNext(callback: V => Unit)

  // Schedules execution of `callback` when completed with final result.
  def onComplete(callback: V => Unit)

}


/**
 * Interface trait for programmatically completing a cell. Analogous to `Promise`.
 */
trait CellCompleter[K, V] {
  def cell: Cell[K, V]

  def putFinal(x: V): Unit
  def putNext(x: V): Unit
}

object CellCompleter {
  def apply[K, V](): CellCompleter[K, V] = {
    ???
  }
}


/* Depend on `cell`. `pred` to decide whether short-cutting is possible. `value` is short-cut result.
 */
class Dep[K, V](val cell: Cell[K, V], val pred: V => Boolean, val value: V)


/* State of a cell that is not yet completed.
 *
 * This is not a case class, since it is important that equality is by-reference.
 *
 * @param res       current intermediate result
 * @param deps      dependencies on other cells
 * @param callbacks list of registered call-back runnables
 */
private class State[K, V](val res: Option[V], val deps: List[Dep[K,V]], val callbacks: List[CallbackRunnable[V]])


abstract class CellImpl[K, V] extends Cell[K, V] with CellCompleter[K, V] {

  /* Contains a value either of type
   * (a) `Try[V]`      for the final result, or
   * (b) `State[K,V]`  for an incomplete state.
   *
   * Assumes that dependencies need to be kept until a final result is known.
   */
  private val state = new AtomicReference[AnyRef]()

  // `CellCompleter` and corresponding `Cell` are the same run-time object.
  def cell: Cell[K, V] = this

  /** Adds dependency on `other` cell. Dependency is removed when `this` cell
   *  is completed. (TODO: distinguish final result from other results?)
   */
  def whenComplete(other: Cell[K, V], pred: V => Boolean, value: V): Unit = {
    state.get() match {
      case finalRes: Try[_]  => // completed with final result
        // do not add dependency
        // in fact, do nothing

      case raw: State[_, _] => // not completed

        /*val subscription =*/ other.onComplete { otherRes =>
          if (pred(otherRes)) this.tryComplete(Success(value))
        }

        // idea: on completion of `this` cell, use `subscription` to cancel completion callback registered with `other`

        val newDep   = new Dep(other, pred, value)
        val current  = raw.asInstanceOf[State[K, V]]
        val newState = new State(current.res, newDep :: current.deps, current.callbacks)
        state.compareAndSet(raw, newState)
    }
  }

  @tailrec
  private def tryCompleteAndGetState(v: Try[V]): State[K, V] = {
    state.get() match {
      case current: State[_, _] =>
        if (state.compareAndSet(current, v))
          current.asInstanceOf[State[K, V]]
        else
          tryCompleteAndGetState(v)

      case _ => null
    }
  }

  def tryComplete(value: Try[V]): Boolean /*= {
    val resolved = resolveTry(value)
    tryCompleteAndGetListeners(resolved) match {
      case null             => false
      case rs if rs.isEmpty => true
      case rs               => rs.foreach(r => r.executeWithValue(resolved)); true
    }
  }*/

  /** Called by `tryComplete` to store the resolved value and get the list of
   *  listeners, or `null` if it is already completed.
   */
  /*@tailrec
  private def tryCompleteAndGetListeners(v: Try[V]): List[CallbackRunnable[T]] = {
    getState match {
      case raw: List[_] =>
        val cur = raw.asInstanceOf[List[CallbackRunnable[T]]]
        if (updateState(cur, v)) cur else tryCompleteAndGetListeners(v)
      case _: DefaultPromise[_] =>
        compressedRoot().tryCompleteAndGetListeners(v)
      case _ => null
    }
  }*/

  // `resolveTry` and `resolver` copied from object `impl.Promise`
  private def resolveTry[T](source: Try[T]): Try[T] = source match {
    case Failure(t) => resolver(t)
    case _          => source
  }

  private def resolver[T](throwable: Throwable): Try[T] = throwable match {
    case t: scala.runtime.NonLocalReturnControl[_] => Success(t.value.asInstanceOf[T])
    case t: scala.util.control.ControlThrowable    => Failure(new ExecutionException("Boxed ControlThrowable", t))
    case t: InterruptedException                   => Failure(new ExecutionException("Boxed InterruptedException", t))
    case e: Error                                  => Failure(new ExecutionException("Boxed Error", e))
    case t                                         => Failure(t)
  }

}


// copied from `impl.CallbackRunnable` in Scala core lib.
private class CallbackRunnable[T](val executor: ExecutionContext, val onComplete: Try[T] => Any) extends Runnable with OnCompleteRunnable {
  // must be filled in before running it
  var value: Try[T] = null

  override def run() = {
    require(value ne null) // must set value to non-null before running!
    try onComplete(value) catch { case NonFatal(e) => executor reportFailure e }
  }

  def executeWithValue(v: Try[T]): Unit = {
    require(value eq null) // can't complete it twice
    value = v
    // Note that we cannot prepare the ExecutionContext at this point, since we might
    // already be running on a different thread!
    try executor.execute(this) catch { case NonFatal(t) => executor reportFailure t }
  }
}
