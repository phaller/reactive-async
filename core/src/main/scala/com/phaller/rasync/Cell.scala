package com.phaller.rasync

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ CountDownLatch, ExecutionException }

import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }
import lattice.{ DefaultKey, Key, PartialOrderingWithBottom, Updater }

import scala.collection.concurrent.TrieMap

trait Cell[K <: Key[V], V] {
  private[rasync] val completer: CellCompleter[K, V]

  def key: K

  /**
   * Returns the current value of `this` `Cell`.
   *
   * Note that this method may return non-deterministic values. To ensure
   * deterministic executions use the quiescence API of class `HandlerPool`.
   */
  def getResult(): V

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
   *   whenComplete(cell, {                   // when `cell` is completed
   *     case Impure => FinalOutcome(Impure)  // if the final value of `cell` is `Impure`, `this` cell is completed with value `Impure`
   *     case _ => NoOutcome
   *   })
   * }}}
   *
   * @param other  Cell that `this` Cell depends on.
   * @param valueCallback  Callback that receives the final value of `other` and returns an `Outcome` for `this` cell.
   */
  def whenComplete(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit
  def whenCompleteSequential(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit

  /**
   * Adds a dependency on some `other` cell.
   *
   * Example:
   * {{{
   *   whenNext(cell, {                       // when the next value is put into `cell`
   *     case Impure => FinalOutcome(Impure)  // if the next value of `cell` is `Impure`, `this` cell is completed with value `Impure`
   *     case _ => NoOutcome
   *   })
   * }}}
   *
   * @param other  Cell that `this` Cell depends on.
   * @param valueCallback  Callback that receives the new value of `other` and returns an `Outcome` for `this` cell.
   */
  def whenNext(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit
  def whenNextSequential(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit

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
   * @param other  Cell that `this` Cell depends on.
   * @param valueCallback  Callback that receives the new value of `other` and returns an `Outcome` for `this` cell.
   */
  def when(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V]): Unit
  def whenSequential(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V]): Unit

  def zipFinal(that: Cell[K, V]): Cell[DefaultKey[(V, V)], (V, V)]

  // internal API

  // Schedules execution of `callback` when next intermediate result is available. (not thread safe!)
  private[rasync] def onNext[U](callback: Try[V] => U): Unit //(implicit context: ExecutionContext): Unit

  // Schedules execution of `callback` when completed with final result. (not thread safe!)
  private[rasync] def onComplete[U](callback: Try[V] => U): Unit

  // Only used in tests.
  private[rasync] def waitUntilNoDeps(): Unit

  // Only used in tests.
  private[rasync] def waitUntilNoNextDeps(): Unit

  /**
   * Returns true, iff computations relevant to this cell are running.
   * A cell becomes active, when it is triggered via its `trigger` method, which in turn
   * calls the `init` method.
   * A cell becomes inactive, when it is completed.
   */
  private[rasync] def tasksActive(): Boolean
  private[rasync] def setTasksActive(): Boolean

  private[rasync] def numTotalDependencies: Int
  private[rasync] def numNextDependencies: Int
  private[rasync] def numCompleteDependencies: Int

  /** Returns the number of cells that depend on this cell. */
  private[rasync] def numNextDependentCells: Int
  /** Returns the number of cells that depend on this cell. */
  private[rasync] def numCompleteDependentCells: Int

  private[rasync] def addCompleteDependentCell(dependentCell: Cell[K, V]): Unit
  private[rasync] def addNextDependentCell(dependentCell: Cell[K, V]): Unit
  private[rasync] def addCombinedDependentCell(dependentCell: Cell[K, V]): Unit

  private[rasync] def removeDependentCell(dependentCell: Cell[K, V]): Unit

  /**
   * Put a final value to `this` cell, but do not propagate to some cells.
   *
   * This is used for cycle resolution to not create cycle propagations.
   *
   * @param value The value to put.
   * @param dontCall The cells that won't be informed about the update.
   */
  private[rasync] def resolveWithValue(value: V, dontCall: Seq[Cell[K, V]]): Unit
  def completeCellDependencies: Seq[Cell[K, V]]
  def totalCellDependencies: Seq[Cell[K, V]]
  private[rasync] def isIndependent(): Boolean

  def removeCompleteCallbacks(cell: Cell[K, V]): Unit
  def removeNextCallbacks(cell: Cell[K, V]): Unit
  def removeCombinedCallbacks(cell: Cell[K, V]): Unit

  /**
   * Removes all callbacks that are called, when `cell` has been updated.
   * I.e., this method kills the dependency on the dependee's side.
   *
   * Use removeDependentCell to remove the dependency on the other side.
   */
  private[rasync] def removeAllCallbacks(cell: Cell[K, V]): Unit
  private[rasync] def removeAllCallbacks(cells: Iterable[Cell[K, V]]): Unit

  /**
   * Inform `this` cell, that `otherCell` has been changed.
   *
   * @param otherCell The cell that received a new value.
   */
  private[rasync] def updateDeps(otherCell: Cell[K, V]): Unit

  /**
   * Remove the staged value `dependentCell` from the associated queue.
   *
   * For each dependet cell, `this` cell has provides a queue that contains the value that
   * should be propagated to `dependentCell`. When `this` cell's value changed, the staged value
   * is updated as well. When `dependetCell` polls a value, the queue is emptied.
   *
   * This method returns
   * - NoOutcome, if the value of `this` cell has not been changed since the last poll.
   * - NextOutcome, if the value of `this` cell has been updated since the last poll but has not been completed.
   * - FinalOutcome, if the value of `this` cell has been updated and completed since the last poll.
   *
   * @param dependentCell The depdent cell that polls a value
   * @param completeDep true, iff the dependency has been established via `whenComplete`.
   * @return An Outcome to propagate to `dependentCell`.
   */
  private[rasync] def pollFor(dependentCell: Cell[K, V], completeDep: Boolean): Outcome[V]
  private[rasync] def peekFor(dependentCell: Cell[K, V], completeDep: Boolean): Outcome[V]

  /**
   * Returns true, iff other cells depend on `this` cell.
   *
   * Note that this method returns non-determinstic results, if the
   * pool is not quiescent.
   *
   * @return true, iff other cells depend on `this` cell.
   */
  def isADependee(): Boolean

  def removeDependency(otherCell: Cell[K, V]): Unit
}

object Cell {

  def completed[V](result: V)(implicit updater: Updater[V], pool: HandlerPool): Cell[DefaultKey[V], V] = {
    val completer = CellCompleter.completed(result)(updater, pool)
    completer.cell
  }

  def sequence[K <: Key[V], V](in: List[Cell[K, V]])(implicit pool: HandlerPool): Cell[DefaultKey[List[V]], List[V]] = {
    implicit val updater: Updater[List[V]] = Updater.partialOrderingToUpdater(PartialOrderingWithBottom.trivial[List[V]])
    val completer =
      CellCompleter[DefaultKey[List[V]], List[V]](new DefaultKey[List[V]])
    in match {
      case List(c) =>
        c.onComplete {
          case Success(x) =>
            completer.putFinal(List(x))
          case f @ Failure(_) =>
            completer.tryComplete(f.asInstanceOf[Failure[List[V]]], None)
        }
      case c :: cs =>
        val fst = in.head
        fst.onComplete {
          case Success(x) =>
            val tailCell = sequence(in.tail)
            tailCell.onComplete {
              case Success(xs) =>
                completer.putFinal(x :: xs)
              case f @ Failure(_) =>
                completer.tryComplete(f, None)
            }
          case f @ Failure(_) =>
            completer.tryComplete(f.asInstanceOf[Failure[List[V]]], None)
        }
    }
    completer.cell
  }

}

/**
 * `CellImpl` uses a `State` to store the current value and dependency information.
 * A state can either be an `IntermediateState` or a `FinalState`.
 */
private trait State[V]

/* State of a cell that is not yet completed.
 *
 * This is not a case class, since it is important that equality is by-reference.
 *
 * @param res       current intermediate result (optional)
 * @param deps      dependent Cells + a staged value for this cell (this is not needed for completeDeps, the staged value is always the final one)
 * @param callbacks those callbacks are run if the cell t(hat we depend on) changes.
 */
private class IntermediateState[K <: Key[V], V](
  /** The result currently stored in the cell.  */
  val res: V,
  /** `true`, if the `init` method has been called.  */
  val tasksActive: Boolean,

  // TODO can those be concurrent data structures? -> remove the need of CAS, in FinalState, we use TrieMaps
  /** A list of cells that depend on `this` cell. */
  val completeDependentCells: List[Cell[K, V]],
  /** A list of cells that `this` cell depends on mapped to the callbacks to call, if those cells change. */
  val completeCallbacks: Map[Cell[K, V], CompleteCallbackRunnable[K, V]],
  /** A list of cells that depend on `this` cell mapped to a one-element-queue that contains a staged value to propagate. */
  val nextDependentCells: Map[Cell[K, V], Option[V]],
  /** A list of cells that `this` cell depends on mapped to the callbacks to call, if those cells change. */
  val nextCallbacks: Map[Cell[K, V], NextCallbackRunnable[K, V]],
  /** A list of cells that `this` cell depends on mapped to the callbacks to call, if those cells change. */
  val combinedCallbacks: Map[Cell[K, V], CombinedCallbackRunnable[K, V]]) extends State[V]

private object IntermediateState {
  def empty[K <: Key[V], V](updater: Updater[V]): IntermediateState[K, V] =
    new IntermediateState[K, V](updater.initial, false, List(), Map(), Map(), Map(), Map())
}

private class FinalState[K <: Key[V], V](
  /** The final result of this cell.*/
  val res: Try[V],
  /*
  * When a cell is completed, all `completeDependentCells` are copied from the IntermediateState
  * to the FinalState. That way, we know that the dependentCell may poll a staged value (at most) one
  * more time. After that, the dependentCell doese not depend on `this` cell any more and is removed
  * from `completeDependentCells`. Repeated polls will return in NoOutcome.
  */
  val completeDependentCells: TrieMap[Cell[K, V], Unit],
  val nextDependentCells: TrieMap[Cell[K, V], Unit])
  extends State[V]

/**
 * Implementation of traits Cell and CellCompleter as the same run-time object.
 *
 * Instances of the class use a `State` to store the current value and dependency information.
 */
private class CellImpl[K <: Key[V], V](pool: HandlerPool, val key: K, updater: Updater[V], override val init: (Cell[K, V]) => Outcome[V]) extends Cell[K, V] with CellCompleter[K, V] {
  implicit val ctx: HandlerPool = pool

  // Used for tests only.
  private val nocompletedepslatch = new CountDownLatch(1)
  // Used for tests only.
  private val nonextdepslatch = new CountDownLatch(1)

  /* Contains a value either of type
   * (a) `FinalState[K, V]`         for the final result, or
   * (b) `IntermediateState[K,V]`   for an incomplete state.
   */
  private val state = new AtomicReference[State[V]](IntermediateState.empty[K, V](updater))

  // A list of callbacks to call, when `this` cell is completed/updated.
  // Note that this is not sync'ed with the state in any way, so calling `onComplete` or `onNext`
  // while tasks are running leads to non-deterministic calls.
  private var onCompleteHandler: List[Try[V] => Any] = List()
  private var onNextHandler: List[Try[V] => Any] = List()

  // `CellCompleter` and corresponding `Cell` are the same run-time object.
  override val cell: Cell[K, V] = this
  override val completer: CellCompleter[K, V] = this.asInstanceOf[CellCompleter[K, V]]

  override def sequential[T](f: => T): T = this.synchronized(f)

  override def getResult(): V = state.get() match {
    case finalRes: FinalState[K, V] =>
      finalRes.res match {
        case Success(result) => result
        case Failure(err) => throw new IllegalStateException(err)
      }
    case raw: IntermediateState[K, V] => raw.res
  }

  override def trigger(): Unit = {
    pool.triggerExecution(this)
  }

  override def isComplete: Boolean = state.get match {
    case _: FinalState[K, V] => true
    case _ => false
  }

  override def putFinal(x: V): Unit = {
    val res = tryComplete(Success(x), None)
    if (!res)
      throw new IllegalStateException("Cell already completed.")
  }

  override def putNext(x: V): Unit = {
    val res = tryNewState(x)
    if (!res)
      throw new IllegalStateException("Cell already completed.")
  }

  override def put(x: V, isFinal: Boolean): Unit = {
    if (isFinal) putFinal(x)
    else putNext(x)
  }

  def zipFinal(that: Cell[K, V]): Cell[DefaultKey[(V, V)], (V, V)] = {
    implicit val theUpdater: Updater[V] = updater
    val completer =
      CellCompleter[DefaultKey[(V, V)], (V, V)](new DefaultKey[(V, V)])(Updater.pair(updater), pool)
    this.onComplete {
      case Success(x) =>
        that.onComplete {
          case Success(y) =>
            completer.putFinal((x, y))
          case f @ Failure(_) =>
            completer.tryComplete(f.asInstanceOf[Try[(V, V)]], None)
        }
      case f @ Failure(_) =>
        completer.tryComplete(f.asInstanceOf[Try[(V, V)]], None)
    }
    completer.cell
  }

  private[this] def currentState(): IntermediateState[K, V] =
    state.get() match {
      case _: FinalState[K, V] => // completed with final result
        null
      case pre: IntermediateState[_, _] => // not completed
        pre.asInstanceOf[IntermediateState[K, V]]
    }

  override private[rasync] def numCompleteDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else (current.completeCallbacks.keys ++ current.combinedCallbacks.keys).toSet.size
  }

  override private[rasync] def numNextDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else (current.nextCallbacks.keys ++ current.combinedCallbacks.keys).toSet.size
  }

  override private[rasync] def numTotalDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else (current.completeCallbacks.keys ++ current.nextCallbacks.keys ++ current.combinedCallbacks.keys).toSet.size
  }

  override def completeCellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: FinalState[K, V] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: IntermediateState[_, _] => // not completed
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        current.completeCallbacks.keys.toSeq
    }
  }

  override def totalCellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: FinalState[K, V] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: IntermediateState[_, _] => // not completed
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        (current.completeCallbacks.keys ++ current.nextCallbacks.keys ++ current.combinedCallbacks.keys).toSeq
    }
  }

  override private[rasync] def isIndependent(): Boolean = {
    state.get() match {
      case finalRes: FinalState[K, V] => // completed with final result
        true
      case pre: IntermediateState[_, _] => // not completed
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        current.combinedCallbacks.isEmpty && current.completeCallbacks.isEmpty && current.nextCallbacks.isEmpty
    }
  }

  override def numNextDependentCells: Int = {
    state.get() match {
      case finalRes: FinalState[K, V] => // completed with final result
        0
      case pre: IntermediateState[_, _] => // not completed
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        current.nextDependentCells.keys.size
    }
  }

  override def numCompleteDependentCells: Int = {
    state.get() match {
      case finalRes: FinalState[K, V] => // completed with final result
        0
      case pre: IntermediateState[_, _] => // not completed
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        current.completeDependentCells.size
    }
  }

  override private[rasync] def resolveWithValue(value: V, dontCall: Seq[Cell[K, V]]): Unit = {
    val res = tryComplete(Success(value), Some(dontCall))
    if (!res)
      throw new IllegalStateException("Cell already completed.")
  }

  override def when(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V]): Unit = {
    this.when(other, valueCallback, sequential = false)
  }

  override def whenSequential(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V]): Unit = {
    this.when(other, valueCallback, sequential = true)
  }

  private def when(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V], sequential: Boolean): Unit = {
    var success = false
    while (!success) { // repeat until compareAndSet succeeded (or the dependency is outdated)
      state.get() match {
        case finalRes: FinalState[K, V] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: IntermediateState[_, _] => // not completed
          val current = raw.asInstanceOf[IntermediateState[K, V]]
          if (current.combinedCallbacks.contains(other))
            success = true // another combined dependency has been registered already. Ignore the new (duplicate) one.
          else {
            val newCallback: CombinedCallbackRunnable[K, V] =
              if (sequential) new CombinedSequentialCallbackRunnable(pool, this, other, valueCallback)
              else new CombinedConcurrentCallbackRunnable(pool, this, other, valueCallback)

            val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks + (other -> newCallback))
            if (state.compareAndSet(current, newState)) {
              success = true
              // Inform `other` that this cell depends on its updates.
              other.addCombinedDependentCell(this)
              // start calculations on `other` so that we eventually get its updates.
              pool.triggerExecution(other)
            }
          }
      }
    }
  }

  override def whenNext(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    this.whenNext(other, valueCallback, sequential = false)
  }

  override def whenNextSequential(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    this.whenNext(other, valueCallback, sequential = true)
  }

  private def whenNext(other: Cell[K, V], valueCallback: V => Outcome[V], sequential: Boolean): Unit = {
    var success = false
    while (!success) { // repeat until compareAndSet succeeded (or the dependency is outdated)
      state.get() match {
        case finalRes: FinalState[K, V] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: IntermediateState[_, _] => // not completed
          val current = raw.asInstanceOf[IntermediateState[K, V]]

          if (current.nextCallbacks.contains(other))
            success = true // another combined dependency has been registered already. Ignore the new (duplicate) one.
          else {
            val newCallback: NextCallbackRunnable[K, V] =
              if (sequential) new NextSequentialCallbackRunnable(pool, this, other, valueCallback)
              else new NextConcurrentCallbackRunnable(pool, this, other, valueCallback)

            val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.nextDependentCells, current.nextCallbacks + (other -> newCallback), current.combinedCallbacks)
            if (state.compareAndSet(current, newState)) {
              success = true
              // Inform `other` that this cell depends on its updates.
              other.addNextDependentCell(this)
              // start calculations on `other` so that we eventually get its updates.
              pool.triggerExecution(other)
            }
          }

      }
    }
  }

  override def whenComplete(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    this.whenComplete(other, valueCallback, false)
  }

  override def whenCompleteSequential(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    this.whenComplete(other, valueCallback, true)
  }

  private def whenComplete(other: Cell[K, V], valueCallback: V => Outcome[V], sequential: Boolean): Unit = {
    var success = false
    while (!success) {
      state.get() match {
        case _: FinalState[K, V] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: IntermediateState[_, _] => // not completed
          val current = raw.asInstanceOf[IntermediateState[K, V]]
          if (current.completeCallbacks.contains(other))
            success = true
          else {
            val newCallback: CompleteCallbackRunnable[K, V] =
              if (sequential) new CompleteSequentialCallbackRunnable(pool, this, other, valueCallback)
              else new CompleteConcurrentCallbackRunnable(pool, this, other, valueCallback)

            val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks + (other -> newCallback), current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks)
            if (state.compareAndSet(current, newState)) {
              success = true
              // Inform `other` that this cell depends on its updates.
              other.addCompleteDependentCell(this)
              // start calculations on `other` so that we eventually get its updates.
              pool.triggerExecution(other)
            }
          }

      }
    }
  }

  override private[rasync] def addCompleteDependentCell(dependentCell: Cell[K, V]): Unit = {
    triggerOrAddCompleteDependentCell(dependentCell)
  }

  override private[rasync] def addNextDependentCell(dependentCell: Cell[K, V]): Unit = {
    triggerOrAddNextDependentCell(dependentCell)
  }

  override private[rasync] def addCombinedDependentCell(dependentCell: Cell[K, V]): Unit = {
    triggerOrAddNextDependentCell(dependentCell)
    triggerOrAddCompleteDependentCell(dependentCell)
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
  private[rasync] final def tryNewState(value: V): Boolean = {
    state.get() match {
      case _: FinalState[K, V] => // completed with final result already
        true // As decided by phaller, we ignore all updates after freeze and do not throw exceptions

      case raw: IntermediateState[_, _] => // not completed
        val current = raw.asInstanceOf[IntermediateState[K, V]]
        val newVal = tryJoin(current.res, value)
        if (current.res != newVal) {

          // create "staging" for dependent cells.
          // To avoid multiple compareAndSets, this is not moved to a different method
          val staged = Some(newVal)
          val newNextDeps = current.nextDependentCells.map {
            case (c, _) => (c, staged)
          }

          val newState = new IntermediateState(newVal, current.tasksActive, current.completeDependentCells, current.completeCallbacks, newNextDeps, current.nextCallbacks, current.combinedCallbacks)

          if (!state.compareAndSet(current, newState)) {
            tryNewState(value)
          } else {
            // CAS was successful, so there was a point in time where `newVal` was in the cell
            // every dependent cell should pull the new value
            onNextHandler.foreach(_.apply(Success(newVal)))
            current.nextDependentCells.foreach(_._1.updateDeps(this))
            true
          }
        } else true
    }
  }

  /**
   * Called by `tryComplete` to store the resolved value and get the current state
   *  or `null` if it is already completed.
   */
  // TODO: take care of compressing root (as in impl.Promise.DefaultPromise)
  @tailrec
  private def tryCompleteAndGetState(v: Try[V]): AnyRef = {
    state.get() match {
      case current: IntermediateState[_, _] =>
        val currentState = current.asInstanceOf[IntermediateState[K, V]]
        val newVal = tryJoin(currentState.res, v.get)

        // Copy completeDependentCells/nextDependentCells from IntermediateState to FinalState.
        val newCompleteDependentCells: TrieMap[Cell[K, V], Unit] = new TrieMap()
        val newNextDependentCells: TrieMap[Cell[K, V], Unit] = new TrieMap()
        currentState.completeDependentCells.foreach(c => newCompleteDependentCells.put(c, ()))
        currentState.nextDependentCells.foreach(c => newNextDependentCells.put(c._1, ()))
        val newState = new FinalState[K, V](
          Success(newVal),
          newCompleteDependentCells,
          newNextDependentCells)

        if (state.compareAndSet(current, newState))
          (currentState, newState)
        else
          tryCompleteAndGetState(v)

      case finalRes: FinalState[_, _] => finalRes
    }
  }

  override def tryComplete(value: Try[V], dontCall: Option[Seq[Cell[K, V]]]): Boolean = {
    val resolved: Try[V] = resolveTry(value)

    // the only call to `tryCompleteAndGetState`
    val res = tryCompleteAndGetState(resolved) match {
      case _: FinalState[K, V] => // was already complete
        true // As decided by phaller,  we ignore all updates after freeze and do not throw exceptions

      case (pre: IntermediateState[K, V], finalValue: FinalState[K, V]) =>
        // Inform dependent cells about the update.
        val dependentCells = pre.nextDependentCells.keys ++ pre.completeDependentCells
        dontCall match {
          case Some(cells) =>
            dependentCells.foreach(c => if (!cells.contains(c)) c.updateDeps(this))
          case None =>
            dependentCells.foreach(_.updateDeps(this))
        }

        // This cell does not depend on other cells any more.
        pre.combinedCallbacks.keys.foreach(_.removeDependentCell(this))
        pre.nextCallbacks.keys.foreach(_.removeDependentCell(this))
        pre.completeCallbacks.keys.foreach(_.removeDependentCell(this))

        onCompleteHandler.foreach(_.apply(finalValue.res))
        onNextHandler.foreach(_.apply(finalValue.res))

        true
    }
    res
  }

  /**
   * Remove `dependentCell` from the list of dependent cells.
   * Afterwards, `dependentCell` won't get informed about updates of `this` cell any more.
   */
  @tailrec
  private[rasync] final def removeDependentCell(dependentCell: Cell[K, V]): Unit = state.get match {
    case pre: IntermediateState[_, _] =>
      val current = pre.asInstanceOf[IntermediateState[K, V]]
      val newCompleteDeps = current.completeDependentCells.filterNot(_ == dependentCell)
      val newNextDeps = current.nextDependentCells - dependentCell

      val newState = new IntermediateState(current.res, current.tasksActive, newCompleteDeps, current.completeCallbacks, newNextDeps, current.nextCallbacks, current.combinedCallbacks)
      if (!state.compareAndSet(current, newState))
        removeDependentCell(dependentCell) // retry, if compareAndSet failed

    case pre: FinalState[_, _] =>
      // assemble new state
      val current = pre.asInstanceOf[FinalState[K, V]]
      current.nextDependentCells.remove(dependentCell)
      current.completeDependentCells.remove(dependentCell)
  }

  /**
   * Remove `dependentCell` from the list of dependent cells.
   * Afterwards, `dependentCell` won't get informed about final updates of `this` cell any more.
   */
  @tailrec
  override private[rasync] final def removeCompleteDepentCell(dependentCell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newDeps = current.completeDependentCells.filterNot(_ == dependentCell)

        val newState = new IntermediateState(current.res, current.tasksActive, newDeps, current.completeCallbacks, current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeCompleteDepentCell(cell) // retry, if compareAndSet failed
        else if (newDeps.isEmpty)
          nocompletedepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  /**
   * Remove `dependentCell` from the list of dependent cells.
   * Afterwards, `dependentCell` won't get informed about next updates of `this` cell any more.
   */
  @tailrec
  override private[rasync] final def removeNextDepentCell(dependentCell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newNextDeps = current.nextDependentCells - dependentCell

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, newNextDeps, current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextDepentCell(cell) // retry, if compareAndSet failed
        else if (newNextDeps.isEmpty)
          nonextdepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  @tailrec
  override final def removeCompleteCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newCompleteCallbacks = current.completeCallbacks - cell

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, newCompleteCallbacks, current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeCompleteCallbacks(cell)
        else {
          if (newCompleteCallbacks.isEmpty) nocompletedepslatch.countDown()
        }
      case _ => /* do nothing, completed cells do not have callbacks any more. */
    }
  }

  @tailrec
  override final def removeNextCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newNextCallbacks = current.nextCallbacks - cell

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.nextDependentCells, newNextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextCallbacks(cell)
        else {
          if (newNextCallbacks.isEmpty) nonextdepslatch.countDown()
        }
      case _ => /* do nothing, completed cells do not have callbacks any more. */
    }
  }

  @tailrec
  override final def removeCombinedCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newCombinedCallbacks = current.combinedCallbacks - cell

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.nextDependentCells, current.nextCallbacks, newCombinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeCombinedCallbacks(cell)
      case _ => /* do nothing, completed cells do not have callbacks any more. */
    }
  }

  @tailrec
  override private[rasync] final def removeAllCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newNextCallbacks = current.nextCallbacks - cell
        val newCompleteCallbacks = current.completeCallbacks - cell
        val newCombinedCallbacks = current.combinedCallbacks - cell

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, newCompleteCallbacks, current.nextDependentCells, newNextCallbacks, newCombinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeAllCallbacks(cell)
        else {
          if (newNextCallbacks.isEmpty) nonextdepslatch.countDown()
          if (newCompleteCallbacks.isEmpty) nocompletedepslatch.countDown()
        }
      case _ => /* do nothing, completed cells do not have callbacks any more. */
    }
  }

  @tailrec
  override private[rasync] final def removeAllCallbacks(cells: Iterable[Cell[K, V]]): Unit = {
    state.get() match {
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newNextCallbacks = current.nextCallbacks -- cells
        val newCompleteCallbacks = current.completeCallbacks -- cells
        val newCombinedCallbacks = current.combinedCallbacks -- cells

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, newCompleteCallbacks, current.nextDependentCells, newNextCallbacks, newCombinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeAllCallbacks(cells)
        else {
          if (newNextCallbacks.isEmpty) nonextdepslatch.countDown()
          if (newCompleteCallbacks.isEmpty) nocompletedepslatch.countDown()
        }

      case _ => /* do nothing, completed cells do not have callbacks any more. */
    }
  }

  override private[rasync] def waitUntilNoDeps(): Unit = {
    nocompletedepslatch.await()
  }

  override private[rasync] def waitUntilNoNextDeps(): Unit = {
    nonextdepslatch.await()
  }

  override private[rasync] def tasksActive() = state.get() match {
    case _: FinalState[K, V] => false
    case s: IntermediateState[_, _] => s.tasksActive
  }

  /**
   * Mark this cell as "running".
   *
   * @return Returns true, iff the cell's status changed (i.e. it had not been running before).
   */
  @tailrec
  override private[rasync] final def setTasksActive(): Boolean = state.get() match {
    case pre: IntermediateState[_, _] =>
      if (pre.tasksActive)
        false
      else {
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newState = new IntermediateState(current.res, true, current.completeDependentCells, current.completeCallbacks, current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState)) setTasksActive()
        else !pre.tasksActive
      }
    case _ => false
  }

  // Schedules execution of `callback` when next intermediate result is available.
  override private[rasync] def onNext[U](callback: Try[V] => U): Unit = state.get() match {
    case _: IntermediateState[_, _] =>
      if (getResult() != updater.initial) callback(Success(getResult()))
      onNextHandler = callback :: onNextHandler
    case _ => callback(Success(getResult()))
  }

  // Schedules execution of `callback` when completed with final result.
  override def onComplete[U](callback: Try[V] => U): Unit = state.get() match {
    case _: IntermediateState[_, _] =>
      onCompleteHandler = callback :: onCompleteHandler
    case _ =>
      callback(Success(getResult()))
  }

  private[rasync] def pollFor(dependentCell: Cell[K, V], completeDep: Boolean): Outcome[V] = state.get() match {
    case pre: FinalState[K, V] =>
      // assemble new state
      val current = pre.asInstanceOf[FinalState[K, V]]

      if (completeDep) {
        if (current.completeDependentCells.contains(dependentCell)) {
          /* Return v but clear staging before. This avoids repeated invocations of the same callback later. */

          current.completeDependentCells.remove(dependentCell)
          FinalOutcome(current.res.get)
        } else {
          NoOutcome
        }

      } else {
        if (current.nextDependentCells.contains(dependentCell)) {
          /* Return v but clear staging before. This avoids repeated invocations of the same callback later. */

          current.nextDependentCells.remove(dependentCell)
          FinalOutcome(current.res.get)
        } else NoOutcome
      }

    case pre: IntermediateState[_, _] =>
      if (completeDep) {
        NoOutcome // non-final values are not relevant to completeDependencies
      } else {
        val current = pre.asInstanceOf[IntermediateState[K, V]]

        current.nextDependentCells.getOrElse(dependentCell, None) match {
          case Some(v) =>
            // Return v but clear staging before.
            // None signals, that the staged value has been pulled.
            val newNextDependentCells = current.nextDependentCells + (dependentCell -> None)
            val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, newNextDependentCells, current.nextCallbacks, current.combinedCallbacks)
            if (!state.compareAndSet(current, newState)) pollFor(dependentCell, completeDep) // try again
            else NextOutcome(v)
          case None => NoOutcome /* just return that no new value is available. Own state does not need to be changed. */
        }
      }
  }

  private[rasync] def peekFor(dependentCell: Cell[K, V], completeDep: Boolean): Outcome[V] = state.get() match {
    case pre: FinalState[K, V] =>
      val current = pre.asInstanceOf[FinalState[K, V]]

      val valueStaged =
        if (completeDep) current.completeDependentCells.contains(dependentCell)
        else current.nextDependentCells.contains(dependentCell)

      if (valueStaged)
        // Pass the staged value. See "to do" in getStagedValue
        FinalOutcome(current.res.get)
      else
        NoOutcome

    case pre: IntermediateState[_, _] =>
      val current = pre.asInstanceOf[IntermediateState[K, V]]
      val result = current.nextDependentCells.get(dependentCell).map({
        case Some(v) => NextOutcome(v)
        case None => NoOutcome
      }).getOrElse(NoOutcome)
      result
  }

  /**
   * Tries to add ta dependent cell. If already completed, it instantly informs the dependent cell.
   */
  @tailrec
  private def triggerOrAddCompleteDependentCell(dependentCell: Cell[K, V]): Unit = state.get() match {
    case pre: FinalState[K, V] =>
      // depdentCell can update its deps immediately, but
      // we need to stage a value first. (Otherwise, it would
      // receive a NoOutcome via getStagedValueFor(dependentCell))
      val current = pre.asInstanceOf[FinalState[K, V]]
      current.completeDependentCells.put(dependentCell, ())
      dependentCell.updateDeps(this)
    case pre: IntermediateState[_, _] =>
      // assemble new state
      val current = pre.asInstanceOf[IntermediateState[K, V]]
      val newState = new IntermediateState(current.res, current.tasksActive, dependentCell :: current.completeDependentCells, current.completeCallbacks, current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks)
      if (!state.compareAndSet(pre, newState))
        triggerOrAddCompleteDependentCell(dependentCell) // retry, if compareAndSet failed
  }

  /**
   * Tries to add the callback, if already completed, it dispatches the callback to be executed.
   */
  @tailrec
  private def triggerOrAddNextDependentCell(dependentCell: Cell[K, V]): Unit =
    state.get() match {
      case pre: FinalState[K, V] =>
        // depdentCell can update its deps immediately, but
        // we need to stage a value first. (Otherwise, it would
        // receive a NoOutcome via getStagedValueFor(dependentCell))
        val current = pre.asInstanceOf[FinalState[K, V]]
        current.nextDependentCells.put(dependentCell, ())
        dependentCell.updateDeps(this)
      case pre: IntermediateState[_, _] =>
        // assemble new state
        val current = pre.asInstanceOf[IntermediateState[K, V]]

        val staged =
          if (current.res != updater.initial)
            Some(current.res) // If the value of this cell has already been changed before, we can immediately inform the dependent cell about this update.
          else
            None // No update yet

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.nextDependentCells + (dependentCell -> staged), current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(pre, newState))
          triggerOrAddNextDependentCell(dependentCell)
        else // compareAndSet has been successful. Propagate a new value, if present.
        if (staged.isDefined) dependentCell.updateDeps(this)
    }

  // copied from object `impl.Promise`
  private def resolveTry[T](source: Try[T]): Try[T] = source match {
    case Failure(t) => resolver(t)
    case _ => source
  }

  // copied from object `impl.Promise`
  private def resolver[T](throwable: Throwable): Try[T] = throwable match {
    case t: scala.runtime.NonLocalReturnControl[_] => Success(t.value.asInstanceOf[T])
    case t: scala.util.control.ControlThrowable => Failure(new ExecutionException("Boxed ControlThrowable", t))
    case t: InterruptedException => Failure(new ExecutionException("Boxed InterruptedException", t))
    case e: Error => Failure(new ExecutionException("Boxed Error", e))
    case t => Failure(t)
  }

  override private[rasync] def updateDeps(otherCell: Cell[K, V]): Unit = state.get() match {
    case pre: IntermediateState[_, _] =>

      // Store snapshots of the callbacks, as the Cell's callbacks might change
      // before the update (see below) task gets executed
      val current = pre.asInstanceOf[IntermediateState[K, V]]
      val completeCallbacks = current.completeCallbacks.get(otherCell)
      val nextCallbacks = current.nextCallbacks.get(otherCell)
      val combinedCallbacks = current.combinedCallbacks.get(otherCell)

      // eventually check for new values
      pool.execute(() => {
        state.get() match {
          case _: IntermediateState[_, _] =>
            completeCallbacks.foreach { _.run() }
            nextCallbacks.foreach { _.run() }
            combinedCallbacks.foreach { _.run() }
          case _: FinalState[K, V] =>
          /* We are final already, so we ignore all incoming information. */
        }
      })
    case _: FinalState[K, V] => /* We are final already, so we ignore all incoming information. */
  }

  /**
   * Checks if this cell is a dependee of some other cells. This is true if some cells called
   * whenNext[Sequential / Complete](thisCell, f)
   * @return True if some cells depends on this one, false otherwise
   */
  override def isADependee(): Boolean = {
    numCompleteDependentCells > 0 || numNextDependentCells > 0
  }

  override def removeDependency(otherCell: Cell[K, V]): Unit = {
    removeAllCallbacks(otherCell)
    otherCell.removeDependentCell(this)
  }
}
