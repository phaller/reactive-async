package com.phaller.rasync.cell

import com.phaller.rasync.lattice.Updater
import com.phaller.rasync.pool.HandlerPool

import scala.util.{ Failure, Try }

/**
 * Interface trait for programmatically completing a cell. Analogous to `Promise[V]`.
 */
private[rasync] trait CellCompleter[V, E >: Null] {

  /**
   * The cell associated with this completer.
   */
  val cell: Cell[V, E]

  /** A method to call */
  private[rasync] val init: (Cell[V, E]) => Outcome[V]

  /**
   * Update `this` cells value with `x` and freeze it.
   * The new value of `this` cell is determined by its updater.
   */
  def putFinal(x: V): Unit

  /**
   * Update `this` cells value with `x`.
   * The new value of `this` cell is determined by its updater.
   */
  def putNext(x: V): Unit

  /**
   * Update `this` cells value with `x`. If `isFinal` is `true`, the
   * cell will be frozen.
   * The new value of `this` cell is determined by its updater.
   */
  def put(x: V, isFinal: Boolean = false): Unit

  /** Complete the cell without changing its value. */
  def freeze(): Unit

  def putFailure(e: Failure[V]): Unit

  private[rasync] def tryNewState(value: V): Unit
  private[rasync] def tryComplete(value: Try[V], dontCall: Option[Seq[Cell[V, E]]]): Unit

  /**
   * Run code for `this` cell sequentially.
   */
  private[rasync] def sequential(f: () => _, prio: Int): Unit
}

object CellCompleter {
  /**
   * Create a completer for a cell holding values of type `V`
   * given a `HandlerPool` and a `Key[V]`.
   */
  def apply[V, E >: Null](init: (Cell[V, E]) => Outcome[V] = (_: Cell[V, E]) => NoOutcome, sequential: Boolean = false, entity: E = null)(implicit updater: Updater[V], pool: HandlerPool[V, E]): CellCompleter[V, E] = {
    val impl =
      if (sequential) new SequentialCellImpl[V, E](pool, updater, init, entity)
      else new ConcurrentCellImpl[V, E](pool, updater, init, entity)
    pool.register(impl)
    impl
  }

  /**
   * Create a cell completer which is already completed with value `result`.
   *
   * Note: there is no `K` type parameter, since we always use type
   * `DefaultKey[V]`, no other key would make sense.
   */
  def completed[V, E >: Null](result: V, entity: E = null)(implicit updater: Updater[V], pool: HandlerPool[V, E]): CellCompleter[V, E] = {
    val impl = new ConcurrentCellImpl[V, E](pool, updater, _ => NoOutcome, entity)
    pool.register(impl)
    impl.putFinal(result)
    impl
  }

}
