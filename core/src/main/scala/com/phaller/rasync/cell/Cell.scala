package com.phaller.rasync
package cell

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import lattice.Updater
import pool.HandlerPool

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.TimeUnit
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
  def getState(): Try[ValueOutcome[V]]

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
  def when(other: Cell[V]*)(valueCallback: Iterable[(Cell[V], Try[ValueOutcome[V]])] => Outcome[V]): Unit

  // internal API

  // Schedules execution of `callback` when next intermediate result is available. (not thread safe!)
  private[rasync] def onNext[U](callback: Try[V] => U): Unit //(implicit context: ExecutionContext): Unit

  // Schedules execution of `callback` when completed with final result. (not thread safe!)
  private[rasync] def onComplete[U](callback: Try[V] => U): Unit

  // Only used in tests.
  private[rasync] def waitUntilNoDeps(): Unit
  private[rasync] def waitUntilNoDeps(timeout: Long, unit: TimeUnit): Unit

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
  private[rasync] def addDependentCell(dependentCell: Cell[V]): Unit
  private[rasync] def removeDependentCell(dependentCell: Cell[V]): Unit
  private[rasync] def removeDependeeCells(otherCells: Iterable[Cell[V]]): Unit

  private[rasync] def addUpdate(dependee: Cell[V]): Unit

  /**
   * Put a final value to `this` cell, but do not propagate to some cells.
   *
   * This is used for cycle resolution to not create cycle propagations.
   *
   * @param value The value to put.
   * @param dontCall The cells that won't be informed about the update.
   */
  private[rasync] def resolveWithValue(value: Try[V], dontCall: Seq[Cell[V]]): Unit
  private[rasync] def isIndependent(): Boolean

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
private class IntermediateState[V](override val res: Success[V], val tasksActive: Boolean, val dependees: Map[Cell[V], CallbackRunnable[V]], val dependers: Set[Cell[V]]) extends State[V]

private object IntermediateState {
  def empty[V](updater: Updater[V]): IntermediateState[V] =
    new IntermediateState[V](Success(updater.bottom), false, Map.empty, Set.empty)
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

  implicit object SeqTaskOrdering extends Ordering[(Int, () => _)] {
    override def compare(x: (Int, () => _), y: (Int, () => _)): Int = x._1 - y._1
  }

  implicit val ctx: HandlerPool[V] = pool

  // Used for tests only.
  private val nodepslatch = new CountDownLatch(1)

  /* Contains a value either of type
   * (a) `FinalState[V]`         for the final result, or
   * (b) `IntermediateState[K,V]`   for an incomplete state.
   */
  private val state: AtomicReference[State[V]] = new AtomicReference(IntermediateState.empty[V](updater))

  private val queuedCallbacks = new AtomicReference(new mutable.PriorityQueue[(Int, () => _)]()(SeqTaskOrdering))

  // A list of callbacks to call, when `this` cell is completed/updated.
  // Note that this is not sync'ed with the state in any way, so calling `onComplete` or `onNext`
  // while tasks are running leads to non-deterministic calls.
  private var onCompleteHandler: List[Try[V] => Any] = List()
  private var onNextHandler: List[Try[V] => Any] = List()

  @tailrec
  override final def sequential(f: () => _, prio: Int): Unit = {
    val currentQ = queuedCallbacks.get()
    val newQ = currentQ.clone()
    newQ.enqueue((prio, f))
    if (queuedCallbacks.compareAndSet(currentQ, newQ)) {
      if (currentQ.isEmpty) startSequentialTask() // we could use the current thread for the first task. Refactor!
    } else sequential(f, prio)
  }

  /** Submit a queued sequential task to the handler pool. Continues submitting until the queue is empty. */
  private def startSequentialTask(): Unit = {
    val cell = this
    queuedCallbacks.get().headOption.foreach({
      case (prio, task) =>
        // Take the first element but do not remove it yet.
        // When other tasks are added they will find this task in the
        // queue and not start a new thread.

        pool.execute(new Runnable {
          override def run(): Unit = {
            try {
              task.apply()
            } catch {
              // An exception thrown in a callback is stored as the final value for the depender
              case e: Exception => cell.putFailure(Failure(e))
            }

            // We have completed the head task.
            // Now remove it from the queue. If there are more elements
            // left, start the next one
            removeCompletedSequentialTaks()
          }
        }, prio)
    })
  }

  /** Call this from a thread that has completed the head of the queued sequential tasks. */
  @tailrec
  private def removeCompletedSequentialTaks(): Unit = {
    // We have completed the head task.
    // Now remove it from the queue. If there are more elements
    // left, start the next one
    val currentQ = queuedCallbacks.get()
    val newQ = currentQ.tail
    if (queuedCallbacks.compareAndSet(currentQ, newQ)) {
      if (newQ.nonEmpty) startSequentialTask()
    } else removeCompletedSequentialTaks()
  }

  override val completer: CellCompleter[V] = this
  override val cell: Cell[V] = this

  override def getResult(): V = state.get.res match {
    case Success(result) => result
    case Failure(err) => throw new IllegalStateException(err)
  }

  override def getTry(): Try[V] =
    state.get().res

  override def getState(): Try[ValueOutcome[V]] = state.get() match {
    case finalRes: FinalState[V] =>
      finalRes.res.map(FinalOutcome(_))
    case current: IntermediateState[V] =>
      current.res.map(NextOutcome(_))
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
    if (isFinal) tryComplete(Success(x), None)
    else tryNewState(x)
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
    else current.dependees.keys.toSeq
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
      current.dependers.size
  }

  override private[rasync] def resolveWithValue(value: Try[V], dontCall: Seq[Cell[V]]): Unit = {
    tryComplete(value, Some(dontCall))
  }

  @tailrec
  override final def when(other: Cell[V]*)(valueCallback: Iterable[(Cell[V], Try[ValueOutcome[V]])] => Outcome[V]): Unit = state.get match {
    case _: FinalState[V] => // completed with final result
    // do not add dependency
    // in fact, do nothing

    case current: IntermediateState[V] => // not completed
      val newCallback =
        if (sequential) new SequentialCallbackRunnable[V](pool, this, other, valueCallback)
        else new ConcurrentCallbackRunnable[V](pool, this, other, valueCallback)

      val newState = new IntermediateState[V](current.res, current.tasksActive, current.dependees ++ other.map(_ → newCallback), current.dependers)
      if (state.compareAndSet(current, newState)) {
        other.foreach(c => {
          c.addDependentCell(this)
          pool.triggerExecution(c)
        })
      } else when(other: _*)(valueCallback)
  }

  @tailrec
  override final private[rasync] def addDependentCell(dependentCompleter: Cell[V]): Unit = state.get match {
    case _: FinalState[V] =>
      // call now!
      dependentCompleter.addUpdate(this)

    case current: IntermediateState[V] =>
      if (!current.dependers.contains(dependentCompleter)) { // ignore duplicates
        val newState = new IntermediateState[V](current.res, current.tasksActive, current.dependees, current.dependers + dependentCompleter)
        if (state.compareAndSet(current, newState)) {

          if (newState.res.value != updater.bottom) {
            // if there has been a change in the past, call callback immediately
            dependentCompleter.addUpdate(this)
          }
        } else addDependentCell(dependentCompleter) // try again
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
      // We only have to continue, if the updated value actually changes the cell;
      // If value is lower or equal current.res, nothing changes.
      if (updatedValue != current.res) {
        val newState = new IntermediateState[V](updatedValue, current.tasksActive, current.dependees, current.dependers)
        if (state.compareAndSet(current, newState)) {
          // We have a new value. Inform dependers
          current.dependers.foreach(_.addUpdate(this))
          onNextHandler.foreach(_.apply(updatedValue))
        } else tryNewState(value) // CAS failed, try again
      }
  }

  private[rasync] override def tryComplete(value: Try[V], dontCall: Option[Seq[Cell[V]]]): Unit = state.get match {
    case _: FinalState[V] => // completed with final result already
    // As decided by phaller, we ignore all updates after freeze and do not throw exceptions

    case current: IntermediateState[V] => // not completed

      val updatedValue: Try[V] = value.map(tryJoin(current.res.value, _))
      val newState = new FinalState[V](updatedValue)
      if (state.compareAndSet(current, newState)) {
        // Cell has been completed successfully
        // Other cells do not need to call us any more
        current.dependees.keys.foreach(_.removeDependentCell(this))

        // Inform all dependent cells, but not those that have been resolved in the same cycle.
        val toCall = dontCall.map(current.dependers -- _).getOrElse(current.dependers)
        toCall.foreach(_.addUpdate(this))

        // for testing
        onNextHandler.foreach(_.apply(updatedValue))
        onCompleteHandler.foreach(_.apply(updatedValue))
        nodepslatch.countDown() // we do not have any deps left
      } else tryComplete(value, dontCall) // CAS failed, try again
  }

  override private[rasync] def addUpdate(dependee: Cell[V]): Unit = state.get() match {
    case _: FinalState[V] =>
    // ignore updates after this cell has been completed
    case current: IntermediateState[V] =>
      current.dependees.get(dependee).foreach(_.addUpdate(dependee))
  }

  /**
   * Remove `dependentCell` from the list of dependent cells.
   * Afterwards, `dependentCell` won't get informed about updates of `this` cell any more.
   */
  @tailrec
  override final private[rasync] def removeDependentCell(dependentCell: Cell[V]): Unit = state.get match {
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
  override private[rasync] final def removeDependeeCells(otherCells: Iterable[Cell[V]]): Unit = state.get match {
    case current: IntermediateState[V] =>

      val newDependees = current.dependees -- otherCells //.filterNot(_ == otherCells)
      val newState = new IntermediateState[V](current.res, current.tasksActive, newDependees, current.dependers)
      if (state.compareAndSet(current, newState)) {
        if (newState.dependees.isEmpty)
          nodepslatch.countDown()
      } else removeDependeeCells(otherCells)

    case _: FinalState[V] => /* we do not have any information stored anyway. */
  }

  override private[rasync] def waitUntilNoDeps(): Unit = {
    nodepslatch.await()
  }

  override private[rasync] def waitUntilNoDeps(timeout: Long, unit: TimeUnit): Unit = {
    nodepslatch.await(timeout, unit)
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
    case current: IntermediateState[V] =>
      if (current.tasksActive)
        false // Cell has been active before
      else {
        val newState = new IntermediateState(current.res, true, current.dependees, current.dependers)
        if (!state.compareAndSet(current, newState)) setTasksActive() // update failed, retry
        else !current.tasksActive // return TRUE, iff previous value is FALSE
      }
    case _ => false // Cell is final already
  }

  // Schedules execution of `callback` when next intermediate result is available.
  override private[rasync] def onNext[U](callback: Try[V] => U): Unit = state.get match {
    case _: IntermediateState[_] =>
      if (getResult() != updater.bottom) callback(Success(getResult()))
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

}

private[rasync] class SequentialCellImpl[V](pool: HandlerPool[V], updater: Updater[V], override val init: (Cell[V]) => Outcome[V]) extends CellImpl[V](pool, updater, init) with SequentialCell[V]
private[rasync] class ConcurrentCellImpl[V](pool: HandlerPool[V], updater: Updater[V], override val init: (Cell[V]) => Outcome[V]) extends CellImpl[V](pool, updater, init) with ConcurrentCell[V]
