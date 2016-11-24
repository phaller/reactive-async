package cell

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CountDownLatch, ExecutionException}

import scala.annotation.tailrec

import scala.concurrent.{ExecutionContext, OnCompleteRunnable}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

import lattice.{Lattice, LatticeViolationException, Key, DefaultKey}


sealed trait WhenNextPredicate
case object WhenNext extends WhenNextPredicate
case object WhenNextComplete extends WhenNextPredicate
case object FalsePred extends WhenNextPredicate


trait Cell[K <: Key[V], V] {

  def key: K

  /** Returns the current value of `this` `Cell`.
    *
    * Note that this method may return non-deterministic values. To ensure
    * deterministic executions use the quiescence API of class `HandlerPool`.
    */
  def getResult(): V

  def isComplete: Boolean

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
  def whenComplete(other: Cell[K, V], pred: V => Boolean, value: Option[V]): Unit

  /**
   * Adds a dependency on some `other` cell.
   *
   * Example:
   *   whenNext(cell, x => !x, Impure) // if a preliminary result is put in `cell` using 
   *                                   // `putNext`and the predicate is true (meaning `cell`
   *                                   // is impure), `this`cell can receive next intermediate
   *                                   // result with constant `Impure`
   *
   * @param other  Cell that `this` Cell depends on.
   * @param pred   Predicate used to decide whether an intermediate or final result of `this` Cell can be computed early, depending on what the predicate returns.
   *               `pred` is applied to value of `other` cell.
   * @param value  Early result value.
   */
  def whenNext(other: Cell[K, V], pred: V => WhenNextPredicate, value: Option[V]): Unit

  def zipFinal(that: Cell[K, V]): Cell[DefaultKey[(V, V)], (V, V)]

  /**
   * Registers a call-back function to be invoked when quiescence is reached, but `this` cell has not been
   * completed, yet. The call-back function is passed a sequence of the cells that `this` cell depends on.
   */
  // def onCycle(callback: Seq[Cell[K, V]] => V)

  // internal API

  // Schedules execution of `callback` when next intermediate result is available.
  def onNext[U](callback: Try[V] => U): Unit //(implicit context: ExecutionContext): Unit

  // Schedules execution of `callback` when completed with final result.
  def onComplete[U](callback: Try[V] => U): Unit

  def waitUntilNoDeps(): Unit
  def waitUntilNoNextDeps(): Unit

  private[cell] def numTotalDependencies: Int
  private[cell] def numNextDependencies: Int
  private[cell] def numCompleteDependencies: Int

  private[cell] def numNextCallbacks: Int
  private[cell] def numCompleteCallbacks: Int

  private[cell] def addCallback[U](callback: Try[V] => U, cell: Cell[K, V]): Unit
  private[cell] def addNextCallback[U](callback: Try[V] => U, cell: Cell[K, V]): Unit

  private[cell] def resolveWithValue(value: V): Unit
  private[cell] def cellDependencies: Seq[Cell[K, V]]
  private[cell] def totalCellDependencies: Seq[Cell[K, V]]

  private[cell] def removeCompleteCallbacks(cell: Cell[K, V]): Unit
  private[cell] def removeNextCallbacks(cell: Cell[K, V]): Unit
}

object Cell {

  def completed[V](pool: HandlerPool, result: V)(implicit lattice: Lattice[V]): Cell[DefaultKey[V], V] = {
    val completer = CellCompleter.completed(pool, result)
    completer.cell
  }

  def sequence[K <: Key[V], V](in: List[Cell[K, V]])(implicit pool: HandlerPool): Cell[DefaultKey[List[V]], List[V]] = {
    implicit val lattice: Lattice[List[V]] = Lattice.trivial[List[V]]
    val completer =
      CellCompleter[DefaultKey[List[V]], List[V]](pool, new DefaultKey[List[V]])
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


/**
 * Interface trait for programmatically completing a cell. Analogous to `Promise[V]`.
 */
trait CellCompleter[K <: Key[V], V] {

  /**
   * The cell associated with this completer.
   */
  def cell: Cell[K, V]

  def putFinal(x: V): Unit
  def putNext(x: V): Unit

  def tryNewState(value: V): Boolean
  def tryComplete(value: Try[V]): Boolean

  private[cell] def removeDep(cell: Cell[K, V]): Unit
  private[cell] def removeNextDep(cell: Cell[K, V]): Unit
}

object CellCompleter {

  /**
   * Create a completer for a cell holding values of type `V`
   * given a `HandlerPool` and a `Key[V]`.
   */
  def apply[K <: Key[V], V](pool: HandlerPool, key: K)(implicit lattice: Lattice[V]): CellCompleter[K, V] = {
    val impl = new CellImpl[K, V](pool, key, lattice)
    pool.register(impl)
    impl
  }

  // note:
  // no K type parameter, since we always use type DefaultKey[V],
  // no other key would make sense
  def completed[V](pool: HandlerPool, result: V)(implicit lattice: Lattice[V]): CellCompleter[DefaultKey[V], V] = {
    val impl = new CellImpl[DefaultKey[V], V](pool, new DefaultKey[V], lattice)
    pool.register(impl)
    impl.putFinal(result)
    impl
  }

}


/* Depend on `cell`. `pred` to decide whether short-cutting is possible. `value` is short-cut result.
 */
class Dep[K <: Key[V], V](val cell: Cell[K, V], val pred: V => Boolean, val value: V)


/* State of a cell that is not yet completed.
 *
 * This is not a case class, since it is important that equality is by-reference.
 *
 * @param res       current intermediate result (optional)
 * @param deps      dependencies on other cells
 * @param callbacks list of registered call-back runnables
 */
private class State[K <: Key[V], V](
  val res: V,
  val deps: Map[Cell[K, V], List[CompleteDepRunnable[K, V]]],
  val callbacks: Map[Cell[K, V], List[CompleteCallbackRunnable[K, V]]],
  val nextDeps: Map[Cell[K, V], List[NextDepRunnable[K, V]]],
  val nextCallbacks: Map[Cell[K, V], List[NextCallbackRunnable[K, V]]]
)

private object State {
  def empty[K <: Key[V], V](lattice: Lattice[V]): State[K, V] =
    new State[K, V](lattice.empty, Map(), Map(), Map(), Map())
}

class CellImpl[K <: Key[V], V](pool: HandlerPool, val key: K, lattice: Lattice[V]) extends Cell[K, V] with CellCompleter[K, V] {

  private val nodepslatch = new CountDownLatch(1)
  private val nonextdepslatch = new CountDownLatch(1)

  /* Contains a value either of type
   * (a) `Try[V]`      for the final result, or
   * (b) `State[K,V]`  for an incomplete state.
   *
   * Assumes that dependencies need to be kept until a final result is known.
   */
  private val state = new AtomicReference[AnyRef](State.empty[K, V](lattice))

  // `CellCompleter` and corresponding `Cell` are the same run-time object.
  override def cell: Cell[K, V] = this

  override def getResult(): V = state.get() match {
    case finalRes: Try[V] =>
      finalRes match {
        case Success(result) => result
        case Failure(err) => throw new IllegalStateException(err)
      }
    case raw: State[K, V] => raw.res
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

  def zipFinal(that: Cell[K, V]): Cell[DefaultKey[(V, V)], (V, V)] = {
    implicit val theLattice: Lattice[V] = lattice
    val completer =
      CellCompleter[DefaultKey[(V, V)], (V, V)](pool, new DefaultKey[(V, V)])
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
      case finalRes: Try[_] => // completed with final result
        null
      case pre: State[_, _] => // not completed
        pre.asInstanceOf[State[K, V]]
    }

  override def numCompleteDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else current.deps.values.flatten.size
  }

  override def numNextDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else current.nextDeps.values.flatten.size
  }

  override def numTotalDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    (current.deps.values.flatten ++ current.nextDeps.values.flatten).size
  }

  override private[cell] def cellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.deps.keys.toSeq
    }
  }

  override private[cell] def totalCellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.deps.keys.toSeq ++ current.nextDeps.keys.toSeq
    }
  }

  override def numNextCallbacks: Int = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
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
        current.callbacks.values.size
    }
  }

  override private[cell] def resolveWithValue(value: V): Unit = {
    this.putFinal(value)
  }

  /** Adds dependency on `other` cell: when `other` cell receives an intermediate result by using
   *  `putNext`, evaluate `pred` with the result of `other`. If this evaluation yields `WhenNext`
   *  or `WhenNextComplete`, `this` cell receives an intermediate or a final result `value`
   *  respectively.
   *
   *  If `value` is `Some(v)`, then the shortcut value is `v`. Otherwise if `value` is `None`,
   *  then the shortcut value is the same value as the value `other` receives when the
   *  whenNext dependency is triggered.
   *
   *  The thereby introduced dependency is removed when `this` cell
   *  is completed (either prior or after an invocation of `whenNext`).
   */
  override def whenNext(other: Cell[K, V], pred: V => WhenNextPredicate, value: Option[V]): Unit = {
    var success = false
    while (!success) {
      state.get() match {
        case finalRes: Try[_] => // completed with final result
        // do not add dependency
        // in fact, do nothing
          success = true

        case raw: State[_, _] => // not completed
          val newDep = new NextDepRunnable(pool, other, pred, value, this)
          // TODO: it looks like `newDep` is wrapped into a CallbackRunnable by `onComplete` -> bad

          val current = raw.asInstanceOf[State[K, V]]
          val newState = current.nextDeps.contains(other) match {
            case true => new State(current.res, current.deps, current.callbacks, current.nextDeps + (other -> (newDep :: current.nextDeps(other))), current.nextCallbacks)
            case false => new State(current.res, current.deps, current.callbacks, current.nextDeps + (other -> List(newDep)), current.nextCallbacks)
          }
          if (state.compareAndSet(current, newState)) {
            success = true
            other.addNextCallback(newDep, this)
          }
      }
    }
  }

  /** Adds dependency on `other` cell: when `other` cell is completed, evaluate `pred`
   *  with the result of `other`. If this evaluation yields true, complete `this` cell
   *  with `value`.
   *
   *  The thereby introduced dependency is removed when `this` cell
   *  is completed (either prior or after an invocation of `whenComplete`).
   */
  override def whenComplete(other: Cell[K, V], pred: V => Boolean, value: Option[V]): Unit = {
    state.get() match {
      case finalRes: Try[_]  => // completed with final result
        // do not add dependency
        // in fact, do nothing

      case raw: State[_, _] => // not completed
        val newDep = new CompleteDepRunnable(pool, other, pred, value, this)
        // TODO: it looks like `newDep` is wrapped into a CallbackRunnable by `onComplete` -> bad
        other.addCallback(newDep, this)

        val current  = raw.asInstanceOf[State[K, V]]
        val newState = current.deps.contains(other) match {
          case true => new State(current.res, current.deps + (other -> (newDep :: current.deps(other))), current.callbacks, current.nextDeps, current.nextCallbacks)
          case false => new State(current.res, current.deps + (other -> List(newDep)), current.callbacks, current.nextDeps, current.nextCallbacks)
        }
        state.compareAndSet(current, newState)
    }
  }

  override private[cell] def addCallback[U](callback: Try[V] => U, cell: Cell[K, V]): Unit = {
    val runnable = new CompleteCallbackRunnable[K, V](pool, callback, cell)
    dispatchOrAddCallback(runnable)
  }

  override private[cell] def addNextCallback[U](callback: Try[V] => U, cell: Cell[K, V]): Unit = {
    val runnable = new NextCallbackRunnable[K, V](pool, callback, cell)
    dispatchOrAddNextCallback(runnable)
  }

  /** Called by 'putNext' and 'putFinal'. It will try to join the current state
    * with the new value by using the given lattice and return the new value.
    * If 'current == v' then it will return 'v'.
    */
  private def tryJoin(current: V, next: V): V = {
    try {
      lattice.join(current, next)
    } catch {
      case LatticeViolationException(c, n) => current
    }
  }

  /** Called by 'putNext' which will try creating a new state with some new value
    * and then set the new state. The function returns 'true' if it succeeds, 'false'
    * if it fails.
    */
  @tailrec
  final def tryNewState(value: V): Boolean = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result already
        val finalResult = finalRes.asInstanceOf[Try[V]].get
        val newVal = tryJoin(finalResult, value)
        val res = finalRes == Success(newVal)
        if(!res) {
          println(s"problem with $this; existing value: $finalRes, new value: $newVal")
        }
        res
      case raw: State[_, _] => // not completed
        val current = raw.asInstanceOf[State[K, V]]
        val newVal = tryJoin(current.res, value)
        if (current.res != newVal) {
          val newState = new State(newVal, current.deps, current.callbacks, current.nextDeps, current.nextCallbacks)
          if (!state.compareAndSet(current, newState)) {
            tryNewState(value)
          }
          else {
            current.nextCallbacks.values.foreach { callbacks =>
              callbacks.foreach(callback => callback.executeWithValue(Success(value)))
            }
            true
          }
        } else true
    }
  }

  /** Called by `tryComplete` to store the resolved value and get the current state
   *  or `null` if it is already completed.
   */
  // TODO: take care of compressing root (as in impl.Promise.DefaultPromise)
  @tailrec
  private def tryCompleteAndGetState(v: Try[V]): AnyRef = {
    state.get() match {
      case current: State[_, _] =>
        val currentState = current.asInstanceOf[State[K, V]]
        val newVal = Success(tryJoin(currentState.res, v.get))
        if (state.compareAndSet(current, newVal))
          currentState
        else
          tryCompleteAndGetState(v)

      case finalRes: Try[_] => finalRes
    }
  }

  override def tryComplete(value: Try[V]): Boolean = {
    val resolved: Try[V] = resolveTry(value)

    val res = tryCompleteAndGetState(resolved) match {
      case finalRes: Try[_]                          => // was already complete
        val res = finalRes == value
        if (!res) {
          println(s"problem with $this; existing value: $finalRes, new value: $value")
        }
        res

      case pre: State[K, V] =>
        val depsCells = pre.deps.keys
        val nextDepsCells = pre.nextDeps.keys
        if (pre.callbacks.isEmpty) {
          pre.nextCallbacks.values.foreach { callbacks =>
            callbacks.foreach(callback => callback.executeWithValue(resolved.asInstanceOf[Try[V]]))
          }
        } else {
          // NextCallbacks with these cells should not be triggered, because they have
          // an onComplete event that are triggered instead.
          pre.callbacks.values.foreach { callbacks =>
            callbacks.foreach(callback => callback.executeWithValue(resolved.asInstanceOf[Try[V]]))
          }
          pre.nextCallbacks.values.foreach { callbacks =>
            callbacks.foreach { callback =>
              if (!pre.callbacks.contains(callback.dependee))
                callback.executeWithValue(resolved.asInstanceOf[Try[V]])
            }
          }
        }

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
  override private[cell] final def removeDep(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newDeps = current.deps - cell

        val newState = new State(current.res, newDeps, current.callbacks, current.nextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeDep(cell)
        else if (newDeps.isEmpty)
          nodepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[cell] final def removeNextDep(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextDeps = current.nextDeps - cell

        val newState = new State(current.res, current.deps, current.callbacks, newNextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextDep(cell)
        else if (newNextDeps.isEmpty)
          nonextdepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[cell] final def removeCompleteCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newCompleteCallbacks = current.callbacks - cell

        val newState = new State(current.res, current.deps, newCompleteCallbacks, current.nextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeCompleteCallbacks(cell)
      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[cell] final def removeNextCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextCallbacks = current.nextCallbacks - cell

        val newState = new State(current.res, current.deps, current.callbacks, current.nextDeps, newNextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextCallbacks(cell)
      case _ => /* do nothing */
    }
  }

  def waitUntilNoDeps(): Unit = {
    nodepslatch.await()
  }

  def waitUntilNoNextDeps(): Unit = {
    nonextdepslatch.await()
  }

  // Schedules execution of `callback` when next intermediate result is available.
  override def onNext[U](callback: Try[V] => U): Unit = {
    val runnable = new NextCallbackRunnable[K, V](pool, callback, this)
    dispatchOrAddNextCallback(runnable)
  }

  // Schedules execution of `callback` when completed with final result.
  override def onComplete[U](callback: Try[V] => U): Unit = {
    val runnable = new CompleteCallbackRunnable[K, V](pool, callback, this)
    dispatchOrAddCallback(runnable)
  }

  /** Tries to add the callback, if already completed, it dispatches the callback to be executed.
   *  Used by `onComplete()` to add callbacks to a promise and by `link()` to transfer callbacks
   *  to the root promise when linking two promises together.
   */
  @tailrec
  private def dispatchOrAddCallback(runnable: CompleteCallbackRunnable[K, V]): Unit = {
    state.get() match {
      case r: Try[_]  => runnable.executeWithValue(r.asInstanceOf[Try[V]])
      // case _: DefaultPromise[_] => compressedRoot().dispatchOrAddCallback(runnable)
      case pre: State[_, _] =>
        // assemble new state
        val current  = pre.asInstanceOf[State[K, V]]
        val newState = current.callbacks.contains(runnable.cell) match {
          case true => new State(current.res, current.deps, current.callbacks + (runnable.cell -> (runnable :: current.callbacks(runnable.cell))), current.nextDeps, current.nextCallbacks)
          case false => new State(current.res, current.deps, current.callbacks + (runnable.cell -> List(runnable)), current.nextDeps, current.nextCallbacks)
        }
        if (!state.compareAndSet(pre, newState)) dispatchOrAddCallback(runnable)
    }
  }

  /** Tries to add the callback, if already completed, it dispatches the callback to be executed.
    *  Used by `onNext()` to add callbacks to a promise and by `link()` to transfer callbacks
    *  to the root promise when linking two promises together.
    */
  @tailrec
  private def dispatchOrAddNextCallback(runnable: NextCallbackRunnable[K, V]): Unit = {
    state.get() match {
      case r: Try[V]  => runnable.executeWithValue(r.asInstanceOf[Try[V]])
                          /* Cell is completed, do nothing emit an onNext callback */
                         // case _: DefaultPromise[_] => compressedRoot().dispatchOrAddCallback(runnable)
      case pre: State[_, _] =>
        // assemble new state
        val current  = pre.asInstanceOf[State[K, V]]
        val newState = current.nextCallbacks.contains(runnable.dependee) match {
          case true => new State(current.res, current.deps, current.callbacks, current.nextDeps, current.nextCallbacks + (runnable.dependee -> (runnable :: current.nextCallbacks(runnable.dependee))))
          case false => new State(current.res, current.deps, current.callbacks, current.nextDeps, current.nextCallbacks + (runnable.dependee -> List(runnable)))
        }
        if (!state.compareAndSet(pre, newState)) dispatchOrAddNextCallback(runnable)
    }
  }

  // copied from object `impl.Promise`
  private def resolveTry[T](source: Try[T]): Try[T] = source match {
    case Failure(t) => resolver(t)
    case _          => source
  }

  // copied from object `impl.Promise`
  private def resolver[T](throwable: Throwable): Try[T] = throwable match {
    case t: scala.runtime.NonLocalReturnControl[_] => Success(t.value.asInstanceOf[T])
    case t: scala.util.control.ControlThrowable    => Failure(new ExecutionException("Boxed ControlThrowable", t))
    case t: InterruptedException                   => Failure(new ExecutionException("Boxed InterruptedException", t))
    case e: Error                                  => Failure(new ExecutionException("Boxed Error", e))
    case t                                         => Failure(t)
  }

}

private class CompleteDepRunnable[K <: Key[V], V](val pool: HandlerPool,
                                                  val cell: Cell[K, V],
                                                  val pred: V => Boolean,
                                                  val shortCutValue: Option[V],
                                                  val completer: CellCompleter[K, V])
    extends Runnable with OnCompleteRunnable with (Try[V] => Unit) {
  // must be filled in before running it
  var value: Try[V] = null

  override def apply(x: Try[V]): Unit = x match {
    case Success(v) =>
      if (pred(v)) {
        shortCutValue match {
          case Some(scv) => completer.putFinal(scv)
          case None => completer.putFinal(v)
        }
      }
      else {
        completer.removeDep(cell)
        completer.removeNextDep(cell)
      }
    case Failure(e) =>
      completer.removeDep(cell)
      completer.removeNextDep(cell)
  }

  override def run(): Unit = {
    try apply(value) catch { case NonFatal(e) => pool reportFailure e }
  }

  def executeWithValue(v: Try[V]): Unit = {
    value = v
    try pool.execute(this) catch { case NonFatal(t) => pool reportFailure t }
  }
}

// copied from `impl.CallbackRunnable` in Scala core lib.
/**
  * @param executor   The thread that runs the callback function
  * @param onComplete Callback function that is triggered on an onComplete event
  * @param cell       The cell that depends on this callback
  */
private class CompleteCallbackRunnable[K <: Key[V], V](val executor: HandlerPool, val onComplete: Try[V] => Any, val cell: Cell[K, V]) extends Runnable with OnCompleteRunnable {
  // must be filled in before running it
  var value: Try[V] = null

  override def run() = {
    require(value ne null) // must set value to non-null before running!
    try onComplete(value) catch { case NonFatal(e) => executor reportFailure e }
  }

  def executeWithValue(v: Try[V]): Unit = {
    require(value eq null) // can't complete it twice
    value = v
    // Note that we cannot prepare the ExecutionContext at this point, since we might
    // already be running on a different thread!
    try executor.execute(this) catch { case NonFatal(t) => executor reportFailure t }
  }
}

/* Depend on `cell`. `pred` to decide whether short-cutting is possible. `shortCutValue` is short-cut result.
 */
private class NextDepRunnable[K <: Key[V], V](val pool: HandlerPool,
                                              val cell: Cell[K, V],
                                              val pred: V => WhenNextPredicate,
                                              val shortCutValue: Option[V],
                                              val completer: CellCompleter[K, V])
    extends Runnable with OnCompleteRunnable with (Try[V] => Unit) {
  var value: Try[V] = null

  override def apply(x: Try[V]): Unit = {
    x match {
      case Success(v) =>
        pred(v) match {
          case WhenNext =>
            shortCutValue match {
              case Some(scv) => completer.putNext(scv)
              case None => completer.putNext(v)
            }
          case WhenNextComplete =>
            shortCutValue match {
              case Some(scv) => completer.putFinal(scv)
              case None => completer.putFinal(v)
            }
          case _ => /* do nothing */
        }
      case Failure(e) => /* do nothing */
    }
    if (cell.isComplete) completer.removeNextDep(cell)
  }

  override def run(): Unit = {
    try apply(value) catch { case NonFatal(e) => pool reportFailure e }
  }

  def executeWithValue(v: Try[V]): Unit = {
    value = v
    try pool.execute(this) catch { case NonFatal(t) => pool reportFailure t }
  }
}

/**
 * @param executor The thread that runs the callback function
 * @param onNext   Callback function that is triggered on an onNext event
 * @param dependee The cell that depends on this callback
 */
private class NextCallbackRunnable[K <: Key[V], V](val executor: HandlerPool, val onNext: Try[V] => Any, val dependee: Cell[K, V]) {
  def executeWithValue(v: Try[V]): Unit = {
    // Note that we cannot prepare the ExecutionContext at this point, since we might
    // already be running on a different thread!
    try executor.execute(() => onNext(v)) catch { case NonFatal(t) => executor reportFailure t }
  }
}

