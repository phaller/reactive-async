package cell

import lattice.Key

import scala.concurrent.OnCompleteRunnable
import scala.util.{Failure, Success, Try}


/**
  * @param pool The handler pool that runs the callback function
  */
private[cell] abstract class CallbackRunnable[K <: Key[V], V](val pool: HandlerPool) extends Runnable with OnCompleteRunnable {
  def execute(): Unit = {
    pool.execute(this)
  }
}

// copied from `impl.CallbackRunnable` in Scala core lib.
/**
  * @param pool The handler pool that runs the callback function
  * @param dependentCell The cell, that depends on `otherCell`.
  * @param otherCell       The cell that depends on this callback
  * @param onComplete Callback function that is triggered on an onComplete event
  */
// depedentCell is needed to not call whenNext callback, if whenComplete callback exists.
private[cell] class CompleteCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, val dependentCell: Cell[K, V], val otherCell: Cell[K, V], val onComplete: Try[V] => Any) extends CallbackRunnable[K, V](pool) {
  // must be filled in before running it
  var started: Boolean = false

  def run(): Unit = {
    require(!started) // can't complete it twice
    started = true
    onComplete(Success(otherCell.getResult()))
  }
}

/**
  * Dependency between `dependentCompleter` and `otherCell`.
  * @param pool   The handler pool that runs the callback function
  * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
  * @param otherCell The cell that `dependentCompleter` depends on.
  * @param whenCompleteCallback Called to retrieve the new value for the dependent cell.
  */
private[cell] class CompleteDepRunnable[K <: Key[V], V](override val pool: HandlerPool, val dependentCompleter: CellCompleter[K, V], override val otherCell: Cell[K, V], val whenCompleteCallback: V => Outcome[V])
  extends CompleteCallbackRunnable[K, V](pool, dependentCompleter.cell, otherCell, {
    case Success(x) =>
      whenCompleteCallback(x) match {
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
    case Failure(e) =>
      dependentCompleter.removeDep(otherCell)
      dependentCompleter.removeNextDep(otherCell)
  }) {
}

/**
  * @param pool The handler pool that runs the callback function
  * @param dependentCell The cell, that depends on `otherCell`.
  * @param otherCell   Cell that triggers this callback.
  * @param onNext   Callback function that is triggered on an onNext event
  */
// depedentCell is needed to not call whenNext callback, if whenComplete callback exists.
private[cell] class NextCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, val dependentCell: Cell[K, V], otherCell: Cell[K, V], val onNext: Try[V] => Any)
  extends CallbackRunnable[K, V](pool) {

  def run(): Unit = {
    onNext(Success(otherCell.getResult()))
  }
}

/**
  * Dependency between `dependentCompleter` and `otherCell`.
  * @param pool The handler pool that runs the callback function
  * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
  * @param otherCell The cell that `dependentCompleter` depends on.
  * @param whenNextCallback Called to retrieve the new value for the dependent cell.
  */
private[cell] class NextDepRunnable[K <: Key[V], V](override val pool: HandlerPool, val dependentCompleter: CellCompleter[K, V], val otherCell: Cell[K, V], val whenNextCallback: V => Outcome[V])
  extends NextCallbackRunnable[K, V](pool, dependentCompleter.cell, otherCell, t => {
    t match {
      case Success(x) =>
        whenNextCallback(otherCell.getResult()) match {
          case NextOutcome(v) =>
            dependentCompleter.putNext(v)
          case FinalOutcome(v) =>
            dependentCompleter.putFinal(v)
          case _ => /* do nothing */
        }
      case Failure(e) => /* do nothing */
    }

    if (otherCell.isComplete) dependentCompleter.removeNextDep(otherCell)
  })
