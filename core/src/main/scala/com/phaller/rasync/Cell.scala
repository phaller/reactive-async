package com.phaller.rasync

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ CountDownLatch, ExecutionException }

import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }
import lattice.{ DefaultKey, Key, Updater, NotMonotonicException, PartialOrderingWithBottom }

/**
 * A cell is a memory location for values that may be written to by several threads concurrently.
 *
 * The value in a cell can repeatedly be updated monotonically.
 * The computation on a cell eventually ends with a final value.
 * In order to have (quasi-)deterministic final results, all values are required
 * to be taken from a lattice or poset.
 *
 * The computation of a cell's final value can depend on values of other cells. Such dependencies can be declared via
 * {{{
 *   cell.whenNext(other, callback)
 * }}}
 * or the related method `whenComplete`, `whenNextSequential`, `whenCompleteSequential`, `when`, `whenSequential`
 * (see below for details).
 *
 * Whenever the value of the `other` cell changes, `callback` is triggered which retrieves the new value if `other`
 * and returns an `Outcome` to signal how the value of this cell should be updated.
 *
 * - If the callback returns `NoOutcome`, the cell's value should not be changed.
 * - If the callback returns `NextOutcome(v)`, `v` should be put into the cell's storage.
 * - If the callback returns `FinalOutcome(v)`, `v` should be put into the cell's storage and the computation is completed (aka the cell is frozen).
 *
 * The `Updater` object associated with a cell decides, how value `v` and the current value of the cell are merged/combined.
 * - For AggregationUpdateds, if a value `v` is to be put into in cell, the cell's value is set to the `join(currentValue, v)` (with respect to
 * the underlying lattice).
 * - For MonotinicUpdaters, the cell's value is set to `v` as long as the update is monotonic.
 *
 * ==Types of dependecies==
 *
 * To add a dependency on another's cell final value, use
 * {{{
 *   cell.whenComplete(other, callback)
 * }}}
 * registers a dependency whose callback is called only if `other` has been completed with a final value.
 *
 * Additionally,
 * {{{
 *   cell.whenNext(other, callback)
 * }}}
 * registers a dependency of `cell` on `other`, where the callback is called for (intermediate) updates of `other`.
 * The values passed to the callback are non-deterministic. However, the last propagated value is deterministic (and
 * is equal to the value passed to `whenComplete` callbacks.
 *
 * If the computation of a cell's final result uses mutable data, use one of
 * {{{
 *   `cell.whenNextSequential(other, callback)
 *    cell.whenCompleteSequential(other, callback)
 * }}}
 * `whenNextSequential` dependencies behave like `whenNext` dependecies, but all callbacks for `cell` that have been
 * registered for `cell` are run sequentially. For example in
 * {{{
 *   cell.whenNextSequential(other1, callback1)
 *   cell.whenNextSequential(other2, callback2)
 * }}}
 * both `other1` and `other2` might concurrently receive an update. Then `callback1` and `callback2` will not
 * be executed concurrently.
 *
 * Note: `whenComplete` should be used with care, as a cycle in the dependency graph could easily lead to a deadlock,
 * if it consits of such dependencies, while it would be able to make progress if one uses `whenNext` dependencies.
 *
 * Note: `whenNext` should be prefered over `whenNextSequential`, if the callbacks do not use state.
 *
 * The methods
 * {{{
 *   `cell.when(other, callback)
 *    cell.whenSequential(other, callback)
 * }}}
 * register both `whenNext` and `whenComplete` dependencies, the callback retrieves both the new value of
 * `other` and a boolean to indicate, if `other`'s value is final.
 *
 * ==Creation and initialization==
 * All cells are managed by a `HandlerPool`. To create a cell in a such a pool, call
 * {{{val cell = pool.mkCell(key, init)}}}
 * where `init` is a callback function that initializes the cell, that is:
 *
 * - it sets up dependencies on other cells (see above)
 * - it might initialize the state of the computation
 * - it returns an initial value (an `Outcome`) that is put into the cell.
 *
 * ==Callbacks==
 * Dependency callbacks have type `V => Outcome[V]` or `(V, Boolean) => Outcome[V]`.
 * Those function are called after `other` cell is updated.
 * Init callbacks have type `Cell[V] => Outcome[V]` and
 * are called after the calculation of a cell's result has been triggered.
 *
 * In order to keep the computation determinstic, callbacks need to fullfill the following requirements:
 *
 * - The value of this cell should only be changed via the returned `Outcome`.
 * - All mutable data that is used in the callbacks of a cell must not be written to by the callbacks of other cells
 *   (i.e. the state of a computation must be private to the cell).
 * - A callback for `cell` must not "capture" other cells, i.e.
 *     -- it must not directly change the value of another cell (via its `put`, `putNext` or `putFinal` methods).
 *     -- it must not set up dependencies for other cells.
 *
 * Note: A callback might be called with the same value `v` several times.
 *
 * @tparam V Type of the cell's value.
 */

trait Cell[K <: Key[V], V] {
  /* The completer associated with this cell. */
  private[rasync] val completer: CellCompleter[K, V]

  def key: K

  /**
   * Returns the current value of `this` `Cell`.
   *
   * Note that this method may return non-deterministic values. To ensure
   * deterministic executions use the quiescence API of class `HandlerPool`.
   */
  def getResult(): V

  /** Start computations associated with this cell. */
  def trigger(): Unit

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

  /**
   * Adds a sequential dependency on some `other` cell.
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

  /**
   * Adds a sequential dependency on some `other` cell.
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

  def whenNextSequential(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit

  /**
   * Adds a dependency on some `other` cell.
   *
   * Example:
   * {{{
   *   when(cell, (x, isFinal) => x match { // when the next value or final value is put into `cell`
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

  /**
   * Adds a sequential dependency on some `other` cell.
   *
   * Example:
   * {{{
   *   when(cell, (x, isFinal) => x match { // when the next value or final value is put into `cell`
   *     case (_, Impure) => FinalOutcome(Impure)  // if the next value of `cell` is `Impure`, `this` cell is completed with value `Impure`
   *     case (true, Pure) => FinalOutcome(Pure)// if the final value of `cell` is `Pure`, `this` cell is completed with `Pure`.
   *     case _ => NoOutcome
   *   })
   * }}}
   *
   * @param other  Cell that `this` Cell depends on.
   * @param valueCallback  Callback that receives the new value of `other` and returns an `Outcome` for `this` cell.
   */

  def whenSequential(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V]): Unit

  def zipFinal(that: Cell[K, V]): Cell[DefaultKey[(V, V)], (V, V)]

  // internal API

  /** Returns true, if the `init` method associated with this cell has been called. */
  private[rasync] def tasksActive(): Boolean

  /**
   * Mark this cell to indicate, that computations for this cell
   * are in progress.
   *
   * This method is called by a thread before it calls the `init` method. It may proceed to
   * call `init`, iff this method returns true.
   *
   * @return true, iff the cell's status changed (i.e. it had not been running before).
   */
  private[rasync] def setTasksActive(): Boolean

  private[rasync] def numTotalDependencies: Int
  private[rasync] def numNextDependencies: Int
  private[rasync] def numCompleteDependencies: Int

  private[rasync] def addCompleteCallback(callback: CompleteCallbackRunnable[K, V], cell: Cell[K, V]): Unit
  private[rasync] def addNextCallback(callback: NextCallbackRunnable[K, V], cell: Cell[K, V]): Unit

  /*
   * This method is intended to be called by the handler pool,
   * if `this` cell is part of a cycle and recieves a final value during
   * cycle resolution.
   */
  private[rasync] def resolveWithValue(value: V): Unit

  /** Returns a list of all cells that `this` cell depends on for completed values. */
  def cellDependencies: Seq[Cell[K, V]]
  /** Returns a list of all cells that `this` cell depends on. */
  def totalCellDependencies: Seq[Cell[K, V]]
  /** Returns true, iff `this` cell does not depend on other cells. */
  private[rasync] def isIndependent(): Boolean

  private[rasync] def removeCompleteCallbacks(cell: Cell[K, V]): Unit
  def removeNextCallbacks(cell: Cell[K, V]): Unit

  /** Removes all callbacks that target `cell` from `this` cell. */
  private[rasync] def removeAllCallbacks(cell: Cell[K, V]): Unit
  /** Removes all callbacks that target any of the `cells` from `this` cell. */
  private[rasync] def removeAllCallbacks(cells: Seq[Cell[K, V]]): Unit

  /**
   * Checks if this cell is a dependee of some other cells. This is true if some cells called
   * whenNext[Sequential / Complete](thisCell, f) and has not been completed.
   *
   * Note that this method may return non-deterministic values. To ensure
   * deterministic executions use the quiescence API of class `HandlerPool`.
   *
   * @return True if some cells depends on this one, false otherwise
   */
  def isADependee(): Boolean

  // Test API (methods only used in tests)
  private[rasync] def numNextCallbacks: Int
  private[rasync] def numCompleteCallbacks: Int

  // Schedules execution of `callback` when next intermediate result is available.
  private[rasync] def onNext[U](callback: Try[V] => U): Unit //(implicit context: ExecutionContext): Unit

  // Schedules execution of `callback` when completed with final result.
  private[rasync] def onComplete[U](callback: Try[V] => U): Unit

  private[rasync] def waitUntilNoDeps(): Unit
  private[rasync] def waitUntilNoNextDeps(): Unit
}

object Cell {

  /** Returns a completed cell with the given `result`. */
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
            completer.tryComplete(f.asInstanceOf[Failure[List[V]]])
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
                completer.tryComplete(f)
            }
          case f @ Failure(_) =>
            completer.tryComplete(f.asInstanceOf[Failure[List[V]]])
        }
    }
    completer.cell
  }

}

/* State of a cell that is not yet completed.
 *
 * This is not a case class, since it is important that equality is by-reference.
 *
 * @param res                 current intermediate result
 * @param tasksActive         true, iff `init` method has been called
 * @param completeDeps        dependencies on other cell's final values
 * @param completeCallbacks   list of registered callbacks to call, if this cell is completed
 * @param nextDeps            dependencies on other cell's (intermediate) values
 * @param nextCallbacks       list of registered callbacks to call, if this cell is updated
 */
private class State[K <: Key[V], V](
  val res: V,
  val tasksActive: Boolean,
  val completeDeps: Set[Cell[K, V]],
  val completeCallbacks: Map[Cell[K, V], List[CompleteCallbackRunnable[K, V]]],
  val nextDeps: Set[Cell[K, V]],
  val nextCallbacks: Map[Cell[K, V], List[NextCallbackRunnable[K, V]]])

private object State {
  def empty[K <: Key[V], V](updater: Updater[V]): State[K, V] =
    new State[K, V](updater.bottom, false, Set(), Map(), Set(), Map())
}

private class CellImpl[K <: Key[V], V](pool: HandlerPool, val key: K, updater: Updater[V], val init: (Cell[K, V]) => Outcome[V]) extends Cell[K, V] with CellCompleter[K, V] {

  implicit val ctx = pool

  private val nodepslatch = new CountDownLatch(1)
  private val nonextdepslatch = new CountDownLatch(1)

  /* Contains a value either of type
   * (a) `Try[V]`      for the final result, or
   * (b) `State[K,V]`  for an incomplete state.
   *
   * Assumes that dependencies need to be kept until a final result is known.
   */
  private val state = new AtomicReference[AnyRef](State.empty[K, V](updater))

  // `CellCompleter` and corresponding `Cell` are the same run-time object.
  override def cell: Cell[K, V] = this
  override val completer: CellCompleter[K, V] = this

  /**
   * Returns the current value of `this` `Cell`.
   *
   * Note that this method may return non-deterministic values. To ensure
   * deterministic executions use the quiescence API of class `HandlerPool`.
   */
  override def getResult(): V = state.get() match {
    case finalRes: Try[V] =>
      finalRes match {
        case Success(result) => result
        case Failure(err) => throw new IllegalStateException(err)
      }
    case raw: State[K, V] => raw.res
  }

  override def trigger(): Unit = {
    pool.triggerExecution(this)
  }

  override def isComplete: Boolean = state.get match {
    case _: Try[_] => true
    case _ => false
  }

  override def putFinal(x: V): Unit = {
    val res = tryComplete(Success(x))
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
            completer.tryComplete(f.asInstanceOf[Try[(V, V)]])
        }
      case f @ Failure(_) =>
        completer.tryComplete(f.asInstanceOf[Try[(V, V)]])
    }
    completer.cell
  }

  private[this] def currentState(): State[K, V] =
    state.get() match {
      case _: Try[_] => // completed with final result
        null
      case pre: State[_, _] => // not completed
        pre.asInstanceOf[State[K, V]]
    }

  override private[rasync] def numCompleteDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else current.completeDeps.size
  }

  override private[rasync] def numNextDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else current.nextDeps.size
  }

  override private[rasync] def numTotalDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else (current.completeDeps ++ current.nextDeps).size
  }

  override def cellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case _: Try[_] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.completeDeps.toSeq
    }
  }

  override def totalCellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        (current.completeDeps ++ current.nextDeps).toSeq
    }
  }

  override def isIndependent(): Boolean = {
    state.get() match {
      case _: Try[_] =>
        // completed cells do not depend on any other cell any more
        true
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.completeDeps.isEmpty && current.nextDeps.isEmpty
    }
  }

  override def numNextCallbacks: Int = {
    state.get() match {
      case _: Try[_] => // completed with final result
        0
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.nextCallbacks.values.size
    }
  }

  override def numCompleteCallbacks: Int = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        0
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.completeCallbacks.values.size
    }
  }

  override def isADependee(): Boolean = {
    // This implementation assumes that there are no
    // onnext or oncomplete callbacks registered.
    // Those methods are only accessible inside package rasync.
    numCompleteCallbacks > 0 || numNextCallbacks > 0
  }

  override private[rasync] def resolveWithValue(value: V): Unit = {
    this.putFinal(value)
  }

  override def when(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V]): Unit = {
    this.whenNext(other, valueCallback(_, false))
    this.whenComplete(other, valueCallback(_, true))
  }

  override def whenSequential(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V]): Unit = {
    this.whenNextSequential(other, valueCallback(_, false))
    this.whenCompleteSequential(other, valueCallback(_, true))
  }

  override def whenNext(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    this.whenNext(other, valueCallback, sequential = false)
  }

  override def whenNextSequential(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    this.whenNext(other, valueCallback, sequential = true)
  }

  private def whenNext(other: Cell[K, V], valueCallback: V => Outcome[V], sequential: Boolean): Unit = {
    // `success` stores, if a state.compareAndSet has been successful to ensure atomicity of this operation.
    var success = false

    while (!success) {
      state.get() match {
        case finalRes: Try[_] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: State[_, _] => // not completed
          val current = raw.asInstanceOf[State[K, V]]

          // Ensure that `this` cell has the dependency on `other` stored.
          success =
            if (current.nextDeps.contains(other)) true
            else {
              val newState = new State(current.res, current.tasksActive, current.completeDeps, current.completeCallbacks, current.nextDeps + other, current.nextCallbacks)
              state.compareAndSet(current, newState)
            }
          if (success) {
            // Register a callback on `other` that is called, when `other`s value changes.
            val newDep: NextDepRunnable[K, V] =
              if (sequential) new NextSequentialDepRunnable(pool, this, other, valueCallback)
              else new NextConcurrentDepRunnable(pool, this, other, valueCallback)
            other.addNextCallback(newDep, this)

            // We are interested in `other`s value, so computations on `other' have to be started.
            other.trigger()
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
    // `success` stores, if a state.compareAndSet has been successful to ensure atomicity of this operation.
    var success = false

    while (!success) {
      state.get() match {
        case finalRes: Try[_] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: State[_, _] => // not completed
          val current = raw.asInstanceOf[State[K, V]]

          // Ensure that `this` cell has the dependency on `other` stored.
          success =
            if (current.completeDeps.contains(other)) true
            else {
              val newState = new State(current.res, current.tasksActive, current.completeDeps + other, current.completeCallbacks, current.nextDeps, current.nextCallbacks)
              state.compareAndSet(current, newState)
            }
          if (success) {
            // Register a callback on `other` that is called, when `other` is completed.
            val newDep: CompleteDepRunnable[K, V] =
              if (sequential) new CompleteSequentialDepRunnable(pool, this, other, valueCallback)
              else new CompleteConcurrentDepRunnable(pool, this, other, valueCallback)
            other.addCompleteCallback(newDep, this)

            // We are interested in `other`s value, so computation on `other' have to be started.
            pool.triggerExecution(other)
          }
      }
    }
  }

  override private[rasync] def addCompleteCallback(callback: CompleteCallbackRunnable[K, V], cell: Cell[K, V]): Unit = {
    dispatchOrAddCompleteCallback(callback)
  }

  override private[rasync] def addNextCallback(callback: NextCallbackRunnable[K, V], cell: Cell[K, V]): Unit = {
    dispatchOrAddNextCallback(callback)
  }

  /**
   * Called by 'putNext' and 'putFinal'.
   * If an update is possible, this method returns the new value for the cell.
   * Otherwise it throws an exception. E.g. an MonotonicUpdater throws, if
   * `next`  and `current` do not have a upper bound in common.
   */
  private def tryUpdate(current: V, next: V): V = {
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
      case finalRes: Try[_] => // completed with final result already
        // so we just check, if the value does not conflict
        // with the finalResult.
        try {
          val finalResult = finalRes.asInstanceOf[Try[V]].get
          val newVal = tryUpdate(finalResult, value)
          val res = finalRes == Success(newVal)
          res
        } catch {
          case _: NotMonotonicException[_] => false
        }
      case raw: State[_, _] => // not completed
        val current = raw.asInstanceOf[State[K, V]]
        val newVal = tryUpdate(current.res, value)
        if (current.res != newVal) {
          val newState = new State(newVal, current.tasksActive, current.completeDeps, current.completeCallbacks, current.nextDeps, current.nextCallbacks)
          if (!state.compareAndSet(current, newState)) {
            // retry, if compareAndSet was not successful
            tryNewState(value)
          } else {
            // CAS was successful, so `this` cell has
            current.nextCallbacks.values.foreach { callbacks =>
              callbacks.foreach(callback => callback.execute())
            }
            true
          }
        } else {
          // There is no need to do anything, as the `value` would not
          // change the state of this cell. (E.g. it is less than the current result
          // in a monotonic updater.)
          true
        }
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
      case current: State[_, _] =>
        val currentState = current.asInstanceOf[State[K, V]]
        val newVal = Success(tryUpdate(currentState.res, v.get))
        if (state.compareAndSet(current, newVal))
          (currentState, newVal)
        else
          tryCompleteAndGetState(v)

      case finalRes: Try[_] => finalRes
    }
  }

  override def tryComplete(value: Try[V]): Boolean = {
    val resolved: Try[V] = resolveTry(value)

    // the only call to `tryCompleteAndGetState`
    val res = tryCompleteAndGetState(resolved) match {
      case finalRes: Try[_] => // was already complete
        // so we just check, if the value does not conflict
        // with the finalResult.
        val finalResult = finalRes.asInstanceOf[Try[V]].get
        val newVal = value.map(tryUpdate(finalResult, _))
        val res = finalRes == newVal
        res

      case (pre: State[K, V], _: Try[V]) =>
        // `this` cell's value has been changed, so
        // we need to inform depedent cells.
        val nextCallbacks = pre.nextCallbacks
        val completeCallbacks = pre.completeCallbacks

        if (nextCallbacks.nonEmpty)
          nextCallbacks.values.foreach { callbacks =>
            callbacks.foreach(callback => callback.execute())
          }
        if (completeCallbacks.nonEmpty)
          completeCallbacks.values.foreach { callbacks =>
            callbacks.foreach(callback => callback.execute())
          }

        // As we are final now, other cells do not need to inform us about their updates.
        val depsCells = pre.completeDeps
        val nextDepsCells = pre.nextDeps
        if (depsCells.nonEmpty)
          depsCells.foreach(_.removeCompleteCallbacks(this))
        if (nextDepsCells.nonEmpty)
          nextDepsCells.foreach(_.removeNextCallbacks(this))

        true
    }
    if (res) {
      pool.deregister(this)
    }
    res
  }

  @tailrec
  override private[rasync] final def removeDep(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newDeps = current.completeDeps - cell

        val newState = new State(current.res, current.tasksActive, newDeps, current.completeCallbacks, current.nextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeDep(cell)
        else if (newDeps.isEmpty)
          nodepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[rasync] final def removeNextDep(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextDeps = current.nextDeps - cell

        val newState = new State(current.res, current.tasksActive, current.completeDeps, current.completeCallbacks, newNextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextDep(cell)
        else if (newNextDeps.isEmpty)
          nonextdepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  @tailrec
  override final def removeCompleteCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newCompleteCallbacks = current.completeCallbacks - cell

        val newState = new State(current.res, current.tasksActive, current.completeDeps, newCompleteCallbacks, current.nextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeCompleteCallbacks(cell)
      case _ => /* do nothing */
    }
  }

  @tailrec
  override final def removeNextCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextCallbacks = current.nextCallbacks - cell

        val newState = new State(current.res, current.tasksActive, current.completeDeps, current.completeCallbacks, current.nextDeps, newNextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextCallbacks(cell)
      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[rasync] final def removeAllCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextCallbacks = current.nextCallbacks - cell
        val newCompleteCallbacks = current.completeCallbacks - cell

        val newState = new State(current.res, current.tasksActive, current.completeDeps, newCompleteCallbacks, current.nextDeps, newNextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeAllCallbacks(cell)
      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[rasync] final def removeAllCallbacks(cells: Seq[Cell[K, V]]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextCallbacks = current.nextCallbacks -- cells
        val newCompleteCallbacks = current.completeCallbacks -- cells

        val newState = new State(current.res, current.tasksActive, current.completeDeps, newCompleteCallbacks, current.nextDeps, newNextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeAllCallbacks(cells)
      case _ => /* do nothing */
    }
  }

  override private[rasync] def waitUntilNoDeps(): Unit = {
    nodepslatch.await()
  }

  override private[rasync] def waitUntilNoNextDeps(): Unit = {
    nonextdepslatch.await()
  }

  override private[rasync] def tasksActive() = state.get() match {
    case _: Try[_] => false
    case s: State[_, _] => s.tasksActive
  }

  @tailrec
  override private[rasync] final def setTasksActive(): Boolean = state.get() match {
    case pre: State[_, _] =>
      if (pre.tasksActive)
        false
      else {
        val current = pre.asInstanceOf[State[K, V]]
        val newState = new State(current.res, true, current.completeDeps, current.completeCallbacks, current.nextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState)) setTasksActive() // If CAS failed, try again
        else !pre.tasksActive // return true, if tasksActive has been false before
      }
    case _ => false
  }

  // Schedules execution of `callback` when next intermediate result is available.
  override private[rasync] def onNext[U](callback: Try[V] => U): Unit = {
    val runnable = new NextConcurrentCallbackRunnable[K, V](pool, null, this, callback) // NULL indicates that no cell is waiting for this callback.
    dispatchOrAddNextCallback(runnable)
  }

  // Schedules execution of `callback` when completed with final result.
  override def onComplete[U](callback: Try[V] => U): Unit = {
    val runnable = new CompleteConcurrentCallbackRunnable[K, V](pool, null, this, callback) // NULL indicates that no cell is waiting for this callback.
    dispatchOrAddCompleteCallback(runnable)
  }

  /**
   * Tries to add the callback, if already completed, it dispatches the callback to be executed.
   *  Used by `oncomplete` and `whenComplete`-methods.
   */
  @tailrec
  private def dispatchOrAddCompleteCallback(runnable: CompleteCallbackRunnable[K, V]): Unit = {
    state.get() match {
      case r: Try[_] => runnable.execute()
      // case _: DefaultPromise[_] => compressedRoot().dispatchOrAddCompleteCallback(runnable)
      case pre: State[_, _] =>
        // assemble new state
        val current = pre.asInstanceOf[State[K, V]]
        val newState = current.completeCallbacks.contains(runnable.dependentCell) match {
          case true => new State(current.res, current.tasksActive, current.completeDeps, current.completeCallbacks + (runnable.dependentCell -> (runnable :: current.completeCallbacks(runnable.dependentCell))), current.nextDeps, current.nextCallbacks)
          case false => new State(current.res, current.tasksActive, current.completeDeps, current.completeCallbacks + (runnable.dependentCell -> List(runnable)), current.nextDeps, current.nextCallbacks)
        }
        if (!state.compareAndSet(pre, newState)) dispatchOrAddCompleteCallback(runnable)
    }
  }

  /**
   * Tries to add the callback, if already completed, it dispatches the callback to be executed.
   *  Used by `onnext` and `whenNext`-methods.
   */
  @tailrec
  private def dispatchOrAddNextCallback(runnable: NextCallbackRunnable[K, V]): Unit = {
    state.get() match {
      case r: Try[V] => runnable.execute()
      /* Cell is completed, do nothing emit an onNext callback */
      // case _: DefaultPromise[_] => compressedRoot().dispatchOrAddCompleteCallback(runnable)
      case pre: State[_, _] =>
        // assemble new state
        val current = pre.asInstanceOf[State[K, V]]
        val newState = current.nextCallbacks.contains(runnable.dependentCell) match {
          case true => new State(current.res, current.tasksActive, current.completeDeps, current.completeCallbacks, current.nextDeps, current.nextCallbacks + (runnable.dependentCell -> (runnable :: current.nextCallbacks(runnable.dependentCell))))
          case false => new State(current.res, current.tasksActive, current.completeDeps, current.completeCallbacks, current.nextDeps, current.nextCallbacks + (runnable.dependentCell -> List(runnable)))
        }
        if (!state.compareAndSet(pre, newState)) dispatchOrAddNextCallback(runnable)
        else if (current.res != updater.bottom) runnable.execute()
    }
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
}
