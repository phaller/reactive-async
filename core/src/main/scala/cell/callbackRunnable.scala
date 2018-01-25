package cell

import lattice.Key

import scala.concurrent.OnCompleteRunnable
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

/**
 * Run a callback in a handler pool, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[cell] trait CallbackRunnable[K <: Key[V], V] extends Runnable with OnCompleteRunnable {
  /** The handler pool that runs the callback function. */
  val pool: HandlerPool

  /** The cell that triggers the callback. */
  val cell: Cell[K, V]

  /** Add this CallbackRunnable to its handler pool. */
  def execute(): Unit

  /** Essentially, call the callback. */
  override def run(): Unit
}

/**
 * Run a callback concurrently, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[cell] trait ConcurrentCallbackRunnable[K <: Key[V], V] extends CallbackRunnable[K, V] {
  /** Add this CallbackRunnable to its handler pool such that it is run concurrently. */
  def execute(): Unit =
    try pool.execute(this)
    catch { case NonFatal(t) => pool reportFailure t }
}

/**
 * Run a callback sequentially (for a dependent cell), if a value in another cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[cell] trait SequentialCallbackRunnable[K <: Key[V], V] extends CallbackRunnable[K, V] {
  val dependentCell: Cell[K, V]

  /**
   * Add this CallbackRunnable to its handler pool such that it is run sequentially.
   * All SequentialCallbackRunnables with the same `dependentCell` are executed sequentially.
   */
  def execute(): Unit =
    pool.scheduleSequentialCallback(this)
}

/**
 * A dependency between to cells consisting of a dependent cell(completer),
  * an other cell and the callback to calculate new values for the dependent cell.
 */
private[cell] trait Dependency[K <: Key[V], V] {
  val dependentCompleter: CellCompleter[K, V]
  val otherCell: Cell[K, V]
  val valueCallback: V => Outcome[V]
}

/**
 * To be run when `otherCell` gets its final update.
 * @param pool          The handler pool that runs the callback function
 * @param dependentCell The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[cell] abstract class CompleteCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  val dependentCell: Cell[K, V], // needed to not call whenNext callback, if whenComplete callback exists.
  otherCell: Cell[K, V],
  val callback: Try[V] => Any)
  extends CallbackRunnable[K, V] {

  // must be filled in before running it
  var started: Boolean = false

  def run(): Unit = {
    require(!started) // can't complete it twice
    started = true
    callback(Success(otherCell.getResult()))
  }
}

private[cell] class CompleteConcurrentCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCell: Cell[K, V], otherCell: Cell[K, V], override val callback: Try[V] => Any)
  extends CompleteCallbackRunnable[K, V](pool, dependentCell, otherCell, callback) with ConcurrentCallbackRunnable[K, V] {
  override val cell: Cell[K, V] = otherCell
}

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback      Called to retrieve the new value for the dependent cell.
 */
private[cell] abstract class CompleteDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends CompleteCallbackRunnable[K, V](pool, dependentCompleter.cell, otherCell, {
  case Success(x) =>
    valueCallback(x) match {
      case FinalOutcome(v) =>
        dependentCompleter.putFinal(v) // deps will be removed by putFinal()
      case NextOutcome(v) =>
        dependentCompleter.putNext(v)
        dependentCompleter.removeDep(otherCell)
        dependentCompleter.removeNextDep(otherCell)
      case NoOutcome =>
        dependentCompleter.removeDep(otherCell)
        dependentCompleter.removeNextDep(otherCell)
    }
  case Failure(_) =>
    dependentCompleter.removeDep(otherCell)
    dependentCompleter.removeNextDep(otherCell)
}) with Dependency[K, V]

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[cell] class CompleteConcurrentDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends CompleteDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with ConcurrentCallbackRunnable[K, V] {
  override val cell: Cell[K, V] = otherCell
}

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[cell] class CompleteSequentialDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends CompleteDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with SequentialCallbackRunnable[K, V] {
  override val cell: Cell[K, V] = otherCell
}

/**
 * To be run when `otherCell` gets a final update.
 * @param pool          The handler pool that runs the callback function
 * @param dependentCell The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[cell] abstract class NextCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  val dependentCell: Cell[K, V], // needed to not call whenNext callback, if whenComplete callback exists.
  otherCell: Cell[K, V],
  val callback: Try[V] => Any)
  extends CallbackRunnable[K, V] {

  def run(): Unit = {
    callback(Success(otherCell.getResult()))
  }
}

/**
 * @param pool          The handler pool that runs the callback function
 * @param dependentCell The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[cell] class NextConcurrentCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCell: Cell[K, V], otherCell: Cell[K, V], override val callback: Try[V] => Any)
  extends NextCallbackRunnable[K, V](pool, dependentCell, otherCell, callback) with ConcurrentCallbackRunnable[K, V] {
  override val cell: Cell[K, V] = otherCell
}

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[cell] abstract class NextDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends NextCallbackRunnable[K, V](pool, dependentCompleter.cell, otherCell, t => {
  t match {
    case Success(_) =>
      valueCallback(otherCell.getResult()) match {
        case NextOutcome(v) =>
          dependentCompleter.putNext(v)
        case FinalOutcome(v) =>
          dependentCompleter.putFinal(v)
        case _ => /* do nothing */
      }
    case Failure(_) => /* do nothing */
  }

  // Remove the dependency, if `otherCell` is complete.
  // There is no need to removeCompleteDeps, because if those existed,
  // a CompleteDepRunnable would have been called and removed the dep
  if (otherCell.isComplete) dependentCompleter.removeNextDep(otherCell)
}) with Dependency[K, V]

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[cell] class NextConcurrentDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with ConcurrentCallbackRunnable[K, V] {
  override val cell: Cell[K, V] = otherCell
}

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[cell] class NextSequentialDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with SequentialCallbackRunnable[K, V] {
  override val cell: Cell[K, V] = otherCell
}