package com.phaller.rasync
package cell

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import lattice.Updater
import pool.HandlerPool

import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }

trait Cell[V] {
  private[rasync] val completer: CellCompleter[V]

  @volatile var obj: AnyRef = _

  /**
   * Returns the current value of `this` `Cell`.
   *
   * Note that this method may return non-deterministic values. To ensure
   * deterministic executions use the quiescence API of class `HandlerPool`.
   */
  def getResult(): V
  def getTry(): Try[V]
  def getState(): (Try[V], Boolean)

  /**
   * Start computations associated with this cell.
   * The init method is called and relevant cells are triggered recursivly.
   */
  def trigger(): Unit

  /** Return true, iff this cell has been completed. */
  def isComplete: Boolean
  /**
   * Adds a dependency on some `other` cell.
   *
   * Example:
   * {{{
   *   when(cell, { // when the next value or final value is put into `cell`
   *     case (_, Impure) => FinalOutcome(Impure)  // if the next value of `cell` is `Impure`, `this` cell is completed with value `Impure`
   *     case (true, Pure) => FinalOutcome(Pure)// if the final value of `cell` is `Pure`, `this` cell is completed with `Pure`.
   *     case _ => NoOutcome
   *   })
   * }}}
   *
   * @param valueCallback  Callback that receives the new value of `other` and returns an `Outcome` for `this` cell.
   * @param other*  Cells that `this` Cell depends on.
   */
  def when(valueCallback: (Cell[V], Try[ValueOutcome[V]]) => Outcome[V], other: Cell[V]*): Unit

  // internal API

  /**
   * Run code for `this` cell sequentially.
   * @return The result of `f`.
   */
  private[rasync] def sequential[T](f: => T): T

  // Schedules execution of `callback` when next intermediate result is available. (not thread safe!)
  private[rasync] def onNext[U](callback: Try[V] => U): Unit //(implicit context: ExecutionContext): Unit

  // Schedules execution of `callback` when completed with final result. (not thread safe!)
  private[rasync] def onComplete[U](callback: Try[V] => U): Unit

  // Only used in tests.
  private[rasync] def waitUntilNoDeps(): Unit

  /**
   * Returns true, iff computations relevant to this cell are running.
   * A cell becomes active, when it is triggered via its `trigger` method, which in turn
   * calls the `init` method.
   * A cell becomes inactive, when it is completed.
   */
  private[rasync] def tasksActive(): Boolean
  private[rasync] def setTasksActive(): Boolean

  private[rasync] def numDependencies: Int
  private[rasync] def cellDependencies: Seq[Cell[V]]

  /** Returns the number of cells that depend on this cell. */
  private[rasync] def numDependentCells: Int
  private[rasync] def addDependentCell(dependentCell: CellCompleter[V], valueCallback: (Cell[V], Try[ValueOutcome[V]]) => Outcome[V]): Unit
  private[rasync] def removeDependentCell(dependentCell: CellCompleter[V]): Unit
  private[rasync] def removeDependeeCell(otherCell: Cell[V]): Unit

  /**
   * Put a final value to `this` cell, but do not propagate to some cells.
   *
   * This is used for cycle resolution to not create cycle propagations.
   *
   * @param value The value to put.
   * @param dontCall The cells that won't be informed about the update.
   */
  private[rasync] def resolveWithValue(value: V, dontCall: Seq[CellCompleter[V]]): Unit
  private[rasync] def isIndependent(): Boolean

  /**
   * Returns true, iff other cells depend on `this` cell.
   *
   * Note that this method returns non-determinstic results, if the
   * pool is not quiescent.
   *
   * @return true, iff other cells depend on `this` cell.
   */
  def isADependee(): Boolean

  def removeDependency(otherCell: Cell[V]): Unit

  protected val sequential: Boolean
}

trait SequentialCell[V] extends Cell[V] {
  override val sequential = true
}
trait ConcurrentCell[V] extends Cell[V] {
  override val sequential = false
}

/**
 * `CellImpl` uses a `State` to store the current value and dependency information.
 * A state can either be an `IntermediateState` or a `FinalState`.
 */
private trait State[V] {
  val res: Try[V]
}

/* State of a cell that is not yet completed.
 *
 * This is not a case class, since it is important that equality is by-reference.
 *
 * @param res       current intermediate result (optional)
 * @param deps      dependent Cells + a staged value for this cell (this is not needed for completeDeps, the staged value is always the final one)
 * @param callbacks those callbacks are run if the cell t(hat we depend on) changes.
 */
private class IntermediateState[V](
  /** The result currently stored in the cell.  */
  override val res: Success[V],
  /** `true`, if the `init` method has been called.  */
  val tasksActive: Boolean,
  /** A list of cells that `this` cell depends on. */
  val dependees: Set[Cell[V]],
  /** A list of cells that depend on `this` cell mapped to the callbacks to call, if `this` cell changes. */
  // The values of `callbacks` have been plain Functions but have now been wrapped into CallbackRunnables to ensure that
  // that the callbacks are callled at most once for a final value
  val dependers: Map[CellCompleter[V], CallbackRunnable[V]]) extends State[V]

private object IntermediateState {
  def empty[V](updater: Updater[V]): IntermediateState[V] =
    new IntermediateState[V](Success(updater.initial), false, Set.empty, Map.empty)
}

private class FinalState[V](
  /** The final result of this cell.*/
  override val res: Try[V]
/*
  * When a cell is completed, all `completeDependentCells` are copied from the IntermediateState
  * to the FinalState. That way, we know that the dependentCell may poll a staged value (at most) one
  * more time. After that, the dependentCell doese not depend on `this` cell any more and is removed
  * from `completeDependentCells`. Repeated polls will return in NoOutcome.
  */
//  val completeDependentCells: TrieMap[Cell[V], Unit],
//  val nextDependentCells: TrieMap[Cell[V], Unit]
) extends State[V]

/**
 * Implementation of traits Cell and CellCompleter as the same run-time object.
 *
 * Instances of the class use a `State` to store the current value and dependency information.
 */
private[rasync] abstract class CellImpl[V](pool: HandlerPool[V], updater: Updater[V], override val init: (Cell[V]) => Outcome[V]) extends Cell[V] with CellCompleter[V] {
  implicit val ctx: HandlerPool[V] = pool

  // Used for tests only.
  private val nodepslatch = new CountDownLatch(1)

  /* Contains a value either of type
   * (a) `FinalState[V]`         for the final result, or
   * (b) `IntermediateState[K,V]`   for an incomplete state.
   */
  private val state: AtomicReference[State[V]] = new AtomicReference(IntermediateState.empty[V](updater))

  // A list of callbacks to call, when `this` cell is completed/updated.
  // Note that this is not sync'ed with the state in any way, so calling `onComplete` or `onNext`
  // while tasks are running leads to non-deterministic calls.
  private var onCompleteHandler: List[Try[V] => Any] = List()
  private var onNextHandler: List[Try[V] => Any] = List()

  override def sequential[T](f: => T): T = this.synchronized(f)

  override val completer: CellCompleter[V] = this
  override val cell: Cell[V] = this

  override def getResult(): V = state.get.res match {
    case Success(result) => result
    case Failure(err) => throw new IllegalStateException(err)
  }

  override def getTry(): Try[V] =
    state.get().res

  override def getState(): (Try[V], Boolean) = state.get() match {
    case finalRes: FinalState[V] =>
      (finalRes.res, true)
    case current: IntermediateState[V] =>
      (current.res, false)
  }

  override def trigger(): Unit = {
    pool.triggerExecution(this)
  }

  override def isComplete: Boolean = state.get match {
    case _: FinalState[V] => true
    case _ => false
  }

  override def putFinal(x: V): Unit = {
    tryComplete(Success(x), None)
  }

  override def putNext(x: V): Unit = {
    tryNewState(x)
  }

  override def put(x: V, isFinal: Boolean): Unit = {
    if (isFinal) putFinal(x)
    else putNext(x)
  }

  override def putFailure(x: Failure[V]): Unit = {
    tryComplete(x, None)
  }

  override def freeze(): Unit = {
    // this is the most simple implementation – but does it work with MonotonicUpdater?
    putFinal(getResult())
  }

  private[this] def currentState(): IntermediateState[V] = state.get match {
    case _: FinalState[V] => // completed with final result
      null
    case pre: IntermediateState[_] => // not completed
      pre.asInstanceOf[IntermediateState[V]]
  }

  override private[rasync] def numDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else current.dependees.size
  }

  override private[rasync] def cellDependencies: Seq[Cell[V]] = {
    val current = currentState()
    if (current == null) Seq.empty
    else current.dependees.toSeq
  }

  override private[rasync] def isIndependent(): Boolean = state.get match {
    case _: FinalState[V] => // completed with final result
      true
    case current: IntermediateState[V] => // not completed
      current.dependees.isEmpty
  }

  override def numDependentCells: Int = state.get match {
    case _: FinalState[V] => // completed with final result
      0
    case current: IntermediateState[V] => // not completed
      current.dependers.keys.size
  }

  override private[rasync] def resolveWithValue(value: V, dontCall: Seq[CellCompleter[V]]): Unit = {
    tryComplete(Success(value), Some(dontCall))
  }

  @tailrec
  override final def when(valueCallback: (Cell[V], Try[ValueOutcome[V]]) => Outcome[V], other: Cell[V]*): Unit = state.get match {
    case _: FinalState[V] => // completed with final result
    // do not add dependency
    // in fact, do nothing

    case current: IntermediateState[V] => // not completed

      val newState = new IntermediateState[V](current.res, current.tasksActive, current.dependees ++ other, current.dependers)
      if (state.compareAndSet(current, newState)) {
        other.foreach(c => {
          c.addDependentCell(this, valueCallback)
          pool.triggerExecution(c)
        })
      } else when(valueCallback, other: _*)
  }

  @tailrec
  override final private[rasync] def addDependentCell(dependentCompleter: CellCompleter[V], valueCallback: (Cell[V], Try[ValueOutcome[V]]) => Outcome[V]): Unit = state.get match {
    case _: FinalState[V] =>
      // call now!
      val callbackRunnable =
        if (sequential) new SequentialCallbackRunnable[V](dependentCompleter, this, valueCallback)
        else new ConcurrentCallbackRunnable[V](dependentCompleter, this, valueCallback)
      pool.execute(callbackRunnable)

    case current: IntermediateState[V] =>
      if (!current.dependers.contains(dependentCompleter)) { // ignore duplicates
        val newDepender =
          if (sequential) new SequentialCallbackRunnable[V](dependentCompleter, this, valueCallback)
          else new ConcurrentCallbackRunnable[V](dependentCompleter, this, valueCallback)
        val newState = new IntermediateState[V](current.res, current.tasksActive, current.dependees, current.dependers + (dependentCompleter → newDepender))
        if (state.compareAndSet(current, newState)) {

          if (newState.res.value != updater.initial) {
            // if there has been a change in the past, call callback immediately
            pool.execute(newDepender)
          }
        } else addDependentCell(dependentCompleter, valueCallback) // try again
      }
  }

  /**
   * Called by 'putNext' and 'putFinal'. It will try to join the current state
   * with the new value by using the given updater and return the new value.
   * If 'current == v' then it will return 'v'.
   */
  private def tryJoin(current: V, next: V): V = {
    updater.update(current, next)
  }

  /**
   * Called by 'putNext' which will try creating a new state with some new value
   * and then set the new state. The function returns 'true' if it succeeds, 'false'
   * if it fails.
   */
  @tailrec
  private[rasync] final def tryNewState(value: V): Unit = state.get match {
    case _: FinalState[V] => // completed with final result already
    // As decided by phaller, we ignore all updates after freeze and do not throw exceptions

    case current: IntermediateState[V] => // not completed

      val updatedValue = current.res.map(tryJoin(_, value)).asInstanceOf[Success[V]]
      if (updatedValue != current.res) {
        val newState = new IntermediateState[V](updatedValue, current.tasksActive, current.dependees, current.dependers)
        if (state.compareAndSet(current, newState)) {
          // we have a new value. trigger callbacks
          current.dependers.foreach(dep => pool.execute(dep._2, pool.schedulingStrategy.calcPriority(dep._1.cell, this)))
          onNextHandler.foreach(_.apply(updatedValue))
        } else tryNewState(value) // try again
      }
  }

  override def tryComplete(value: Try[V], dontCall: Option[Seq[CellCompleter[V]]]): Unit = state.get match {
    case _: FinalState[V] => // completed with final result already
    // As decided by phaller, we ignore all updates after freeze and do not throw exceptions

    case current: IntermediateState[V] => // not completed

      val updatedValue: Try[V] = value.map(tryJoin(current.res.value, _))
      val newState = new FinalState[V](updatedValue)
      if (state.compareAndSet(current, newState)) {
        // cell has been completed successfully
        // Other cells do not need to call us any more
        current.dependees.foreach(_.removeDependentCell(this))

        // inform all dependent cells, but not those that have been resolved in the same cycle
        val toCall = dontCall.map(current.dependers -- _).getOrElse(current.dependers)
        toCall.values.foreach(callback => pool.execute(callback, pool.schedulingStrategy.calcPriority(callback.dependentCompleter.cell, this)))

        // for testing
        onNextHandler.foreach(_.apply(updatedValue))
        onCompleteHandler.foreach(_.apply(updatedValue))
        nodepslatch.countDown() // we do not have any deps left
      } else tryComplete(value, dontCall)
  }

  /**
   * Remove `dependentCell` from the list of dependent cells.
   * Afterwards, `dependentCell` won't get informed about updates of `this` cell any more.
   */
  @tailrec
  override final private[rasync] def removeDependentCell(dependentCell: CellCompleter[V]): Unit = state.get match {
    case current: IntermediateState[V] =>
      val newDependers = current.dependers - dependentCell
      val newState = new IntermediateState[V](current.res, current.tasksActive, current.dependees, newDependers)
      if (!state.compareAndSet(current, newState)) {
        removeDependentCell(dependentCell)
      }
    case _: FinalState[V] => /* we do not have any information stored anyway. */
  }

  /**
   * Remove `dependentCell` from the list of dependent cells.
   * Afterwards, `dependentCell` won't get informed about updates of `this` cell any more.
   */
  @tailrec
  override private[rasync] final def removeDependeeCell(otherCell: Cell[V]): Unit = state.get match {
    case current: IntermediateState[V] =>

      val newDependees = current.dependees.filterNot(_ == otherCell)
      val newState = new IntermediateState[V](current.res, current.tasksActive, newDependees, current.dependers)
      if (state.compareAndSet(current, newState)) {
        if (newState.dependees.isEmpty)
          nodepslatch.countDown()
      } else removeDependeeCell(otherCell)

    case _: FinalState[V] => /* we do not have any information stored anyway. */
  }

  override private[rasync] def waitUntilNoDeps(): Unit = {
    nodepslatch.await()
  }

  override private[rasync] def tasksActive() = state.get match {
    case _: FinalState[V] => false
    case s: IntermediateState[_] => s.tasksActive
  }

  /**
   * Mark this cell as "running".
   *
   * @return Returns true, iff the cell's status changed (i.e. it had not been running before).
   */
  @tailrec
  override private[rasync] final def setTasksActive(): Boolean = state.get match {
    case pre: IntermediateState[_] =>
      if (pre.tasksActive)
        false
      else {
        val current = pre.asInstanceOf[IntermediateState[V]]
        val newState = new IntermediateState(current.res, true, current.dependees, current.dependers)
        if (!state.compareAndSet(current, newState)) setTasksActive()
        else !pre.tasksActive
      }
    case _ => false
  }

  // Schedules execution of `callback` when next intermediate result is available.
  override private[rasync] def onNext[U](callback: Try[V] => U): Unit = state.get match {
    case _: IntermediateState[_] =>
      if (getResult() != updater.initial) callback(Success(getResult()))
      onNextHandler = callback :: onNextHandler
    case _ => callback(Success(getResult()))
  }

  // Schedules execution of `callback` when completed with final result.
  override def onComplete[U](callback: Try[V] => U): Unit = state.get match {
    case _: IntermediateState[_] =>
      onCompleteHandler = callback :: onCompleteHandler
    case _ =>
      callback(Success(getResult()))
  }

  /**
   * Checks if this cell is a dependee of some other cells. This is true if some cells called
   * whenNext[Sequential / Complete](thisCell, f)
   * @return True if some cells depends on this one, false otherwise
   */
  override def isADependee(): Boolean = {
    numDependentCells > 0
  }

  override def removeDependency(otherCell: Cell[V]): Unit = {
    removeDependeeCell(otherCell)
    otherCell.removeDependentCell(this)
  }
}

private[rasync] class SequentialCellImpl[V](pool: HandlerPool[V], updater: Updater[V], override val init: (Cell[V]) => Outcome[V]) extends CellImpl[V](pool, updater, init) with SequentialCell[V]
private[rasync] class ConcurrentCellImpl[V](pool: HandlerPool[V], updater: Updater[V], override val init: (Cell[V]) => Outcome[V]) extends CellImpl[V](pool, updater, init) with ConcurrentCell[V]
