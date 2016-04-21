package cell

import java.util.concurrent.atomic._
import java.util.concurrent.{CountDownLatch, ExecutionException}

import scala.annotation.tailrec
import scala.concurrent.{OnCompleteRunnable, ExecutionContext}
import scala.language.implicitConversions
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

import lattice.{Lattice, Key}


sealed trait WhenNextPredicate
case object WhenNext extends WhenNextPredicate
case object WhenNextComplete extends WhenNextPredicate
case object FalsePred extends WhenNextPredicate

/**
 * Example:
 *
 *   val barRetTypeCell: Cell[(Entity, PropertyKind), ObjectType]
 */
trait Cell[K <: Key[V], V] {
  def key: K

  def getResult(): V
  // def property: V
  def isComplete(): Boolean

  def dependencies: Seq[K]
  // def addDependency(other: K)
  // def removeDependency(other: K)

  def nextDependencies: Seq[K]
  def totalDependencies: Seq[K]

  // sobald sich der Wert dieser Cell ändert, müssen die dependee Cells benachrichtigt werden
  // def dependees: Seq[K]
  // def addDependee(k: K): Unit
  // def removeDependee(k: K): Unit

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

  private[cell] def addCallback[U](callback: Try[V] => U, cell: Cell[K, V]): Unit
  private[cell] def addNextCallback[U](callback: Try[V] => U, cellCompleter: CellCompleter[K, V]): Unit

  private[cell] def resolveWithValue(value: V): Unit
  private[cell] def cellDependencies: Seq[Cell[K, V]]
  private[cell] def totalCellDependencies: Seq[Cell[K, V]]
}


/**
 * Interface trait for programmatically completing a cell. Analogous to `Promise`.
 */
trait CellCompleter[K <: Key[V], V] {
  def cell: Cell[K, V]

  def putFinal(x: V): Unit
  def putNext(x: V): Unit

  def tryNewState(value: V): Boolean
  def tryComplete(value: Try[V]): Boolean

  private[cell] def removeDep(dep: CompleteDepRunnable[K, V]): Unit
  private[cell] def removeNextDep(cell: Cell[K, V]): Unit
  private[cell] def removeNextDep(callback: Try[V] => Any): Unit
}

object CellCompleter {
  def apply[K <: Key[V], V](pool: HandlerPool, key: K): CellCompleter[K, V] = {
    val impl = new CellImpl[K, V](pool, key)
    pool.register(impl)
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
private class State[K <: Key[V], V](val res: V, val deps: List[CompleteDepRunnable[K, V]], val callbacks: List[CallbackRunnable[K, V]], val nextDeps: List[NextDepRunnable[K, V]], val nextCallbacks: List[NextCallbackRunnable[K, V]])

private object State {
  def empty[K <: Key[V], V](key: K): State[K, V] =
    new State[K, V](key.lattice.empty, List(), List(), List(), List())
}


class CellImpl[K <: Key[V], V](pool: HandlerPool, val key: K) extends Cell[K, V] with CellCompleter[K, V] {

  private val nodepslatch = new CountDownLatch(1)
  private val nonextdepslatch = new CountDownLatch(1)

  /* Contains a value either of type
   * (a) `Try[V]`      for the final result, or
   * (b) `State[K,V]`  for an incomplete state.
   *
   * Assumes that dependencies need to be kept until a final result is known.
   */
  private val state = new AtomicReference[AnyRef](State.empty[K, V](key))

  // `CellCompleter` and corresponding `Cell` are the same run-time object.
  override def cell: Cell[K, V] = this

  override def getResult(): V = state.get() match {
    case finalRes: Try[V] =>
      finalRes match {
        case Success(result) => result
        case Failure(err) => throw new IllegalStateException(err)
        case _ => key.lattice.empty
      }
    case raw: State[K, V] => raw.res
  }

  override def isComplete(): Boolean = state.get match {
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

  override def dependencies: Seq[K] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[K]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.deps.map(_.cell.key)
    }
  }

  override def totalDependencies: Seq[K] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[K]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.deps.map(_.cell.key) ++ current.nextDeps.map(_.cell.key)
    }
  }

  override private[cell] def cellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.deps.map(_.cell)
    }
  }

  override private[cell] def totalCellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.deps.map(_.cell) ++ current.nextDeps.map(_.cell)
    }
  }

  override def nextDependencies: Seq[K] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[K]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.nextDeps.map(_.cell.key)
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
    while(!success) {
      state.get() match {
        case finalRes: Try[_] => // completed with final result
        // do not add dependency
        // in fact, do nothing
          success = true

        case raw: State[_, _] => // not completed
          val newDep = new NextDepRunnable(pool, other, pred, value, this)
          // TODO: it looks like `newDep` is wrapped into a CallbackRunnable by `onComplete` -> bad

          val current = raw.asInstanceOf[State[K, V]]
          val newState = new State(current.res, current.deps, current.callbacks, newDep :: current.nextDeps, current.nextCallbacks)
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
  override def whenComplete(other: Cell[K, V], pred: V => Boolean, value: V): Unit = {
    state.get() match {
      case finalRes: Try[_]  => // completed with final result
        // do not add dependency
        // in fact, do nothing

      case raw: State[_, _] => // not completed
        val newDep = new CompleteDepRunnable(pool, other, pred, value, this)
        // TODO: it looks like `newDep` is wrapped into a CallbackRunnable by `onComplete` -> bad
        other.addCallback(newDep, this)

        val current  = raw.asInstanceOf[State[K, V]]
        val newState = new State(current.res, newDep :: current.deps, current.callbacks, current.nextDeps, current.nextCallbacks)
        state.compareAndSet(current, newState)
    }
  }

  override private[cell] def addCallback[U](callback: Try[V] => U, cell: Cell[K, V]): Unit = {
    val runnable = new CallbackRunnable[K, V](pool, callback, Some(cell))
    dispatchOrAddCallback(runnable)
  }

  override private[cell] def addNextCallback[U](callback: Try[V] => U, cellCompleter: CellCompleter[K, V]): Unit = {
    val runnable = new NextCallbackRunnable[K, V](pool, callback, Some(cellCompleter))
    dispatchOrAddNextCallback(runnable)
  }

  /** Called by 'putNext' and 'putFinal'. It will try to join the current state
    * with the new value by using the given lattice and return the new value.
    * If 'current == v' then it will return 'v'.
    */
  private def tryJoin(current: V, v: V): V = {
    val newVal = key.lattice.join(current, v)

    newVal match {
      case Some(value) => value
      case None => current
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
            current.nextCallbacks.foreach(r => r.executeWithValue(Success(value)))
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
        if(pre.callbacks.isEmpty) {
          pre.nextCallbacks.foreach(callback => callback.executeWithValue(resolved.asInstanceOf[Try[V]]))
        } else {
          // NextCallbacks with these cells should not be triggered, because they have
          // an onComplete event that are triggered instead.
          val cellsWithCompleteCallbacks = pre.callbacks.map(_.cell)
          pre.callbacks.foreach(r => r.executeWithValue(resolved.asInstanceOf[Try[V]]))
          for(callback <- pre.nextCallbacks) {
            if(!cellsWithCompleteCallbacks.contains(callback.dependee))
              callback.executeWithValue(resolved.asInstanceOf[Try[V]])
          }
        }

        true
    }
    if (res) {
      pool.deregister(this)
    }
    res
  }

  @tailrec
  override private[cell] final def removeDep(dep: CompleteDepRunnable[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newDeps = current.deps.filterNot(_ == dep)

        val newState = new State(current.res, newDeps, current.callbacks, current.nextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeDep(dep)
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
        val newNextDeps = current.nextDeps.filterNot(_.cell == cell)

        val newState = new State(current.res, current.deps, current.callbacks, newNextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextDep(cell)
        else if (newNextDeps.isEmpty)
          nonextdepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[cell] final def removeNextDep(callback: Try[V] => Any): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextDeps = current.nextDeps.filterNot(_ == callback)

        val newState = new State(current.res, current.deps, current.callbacks, newNextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextDep(callback)
        else if (newNextDeps.isEmpty)
          nonextdepslatch.countDown()

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
    val runnable = new NextCallbackRunnable[K, V](pool, callback, None)
    dispatchOrAddNextCallback(runnable)
  }

  // Schedules execution of `callback` when completed with final result.
  override def onComplete[U](callback: Try[V] => U): Unit = {
    val runnable = new CallbackRunnable[K, V](pool, callback, None)
    dispatchOrAddCallback(runnable)
  }

  /** Tries to add the callback, if already completed, it dispatches the callback to be executed.
   *  Used by `onComplete()` to add callbacks to a promise and by `link()` to transfer callbacks
   *  to the root promise when linking two promises together.
   */
  @tailrec
  private def dispatchOrAddCallback(runnable: CallbackRunnable[K, V]): Unit = {
    state.get() match {
      case r: Try[_]  => runnable.executeWithValue(r.asInstanceOf[Try[V]])
      // case _: DefaultPromise[_] => compressedRoot().dispatchOrAddCallback(runnable)
      case pre: State[_, _] =>
        // assemble new state
        val current  = pre.asInstanceOf[State[K, V]]
        val newState = new State(current.res, current.deps, runnable :: current.callbacks, current.nextDeps, current.nextCallbacks)
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
        val newState = new State(current.res, current.deps, current.callbacks, current.nextDeps, runnable :: current.nextCallbacks)
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
                                                  val shortCutValue: V,
                                                  val completer: CellCompleter[K, V])
    extends Runnable with OnCompleteRunnable with (Try[V] => Unit) {
  var value: Try[V] = null

  override def apply(x: Try[V]): Unit = x match {
    case Success(v) =>
      if (pred(v)) completer.putFinal(shortCutValue)
      else {
        completer.removeDep(this)
        completer.removeNextDep(cell)
      }
    case Failure(e) =>
      completer.removeDep(this)
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
  * @param cell       The cell that is depends on this callback
  */
private class CallbackRunnable[K <: Key[V], V](val executor: HandlerPool, val onComplete: Try[V] => Any, val cell: Option[Cell[K, V]]) extends Runnable with OnCompleteRunnable {
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
    if(cell.isComplete()) completer.removeNextDep(cell)
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
 * @param dependee The cell that is depends on this callback
 */
private class NextCallbackRunnable[K <: Key[V], V](val executor: HandlerPool, val onNext: Try[V] => Any, val dependee: Option[CellCompleter[K, V]]) {
  def executeWithValue(v: Try[V]): Unit = {
    // Note that we cannot prepare the ExecutionContext at this point, since we might
    // already be running on a different thread!
    try executor.execute(() => onNext(v)) catch { case NonFatal(t) => executor reportFailure t }
  }
}

