package com.phaller.rasync

import scala.util.Try
import lattice.{ DefaultKey, Key, Updater }

/**
 * Interface trait for programmatically completing a cell. Analogous to `Promise[V]`.
 */
trait CellCompleter[K <: Key[V], V] {

  /**
   * The cell associated with this completer.
   */
  def cell: Cell[K, V]

  private[rasync] def init: (Cell[K, V]) => Outcome[V]

  /**
   * Put `x` into the cell and mark the cell as completed.
   * The cell's updater specifies, how `x` and the current value
   * of the cell interfere.
   */
  def putFinal(x: V): Unit

  /**
   * Put `x` into the cell.
   * The cell's updater specifies, how `x` and the current value
   * of the cell interfere.
   */
  def putNext(x: V): Unit

  /**
   * Put `x` into the cell and, if `isFinal` is true, mark the cell as completed.
   * The cell's updater specifies, how `x` and the current value
   * of the cell interfere.
   */
  def put(x: V, isFinal: Boolean): Unit

  private[rasync] def removeDep(cell: Cell[K, V]): Unit
  private[rasync] def removeNextDep(cell: Cell[K, V]): Unit

  // Test API
  // Those methods are used in tests and could otherwise be private to CellImpl.
  private[rasync] def tryNewState(value: V): Boolean
  private[rasync] def tryComplete(value: Try[V]): Boolean
}

object CellCompleter {

  /**
   * Create a completer for a cell holding values of type `V`.
   *
   * Cells that depend on each other must be handled by the some `pool` in order to
   * track quiescence.
   *
   * @param key the key to resolve this cell with.
   * @param init the method that is called, when the cell's result(s) are needed by some other computation.
   * @param updater defines which update mechanism to use.
   * @param pool defines the pool that handles this cell's computations.
   * @tparam K Type of the cell's key.
   * @tparam V Type of the cell's values.
   * @return a new cell completer.
   */
  def apply[K <: Key[V], V](key: K, init: (Cell[K, V]) => Outcome[V] = (_: Cell[K, V]) => NoOutcome)(implicit updater: Updater[V], pool: HandlerPool): CellCompleter[K, V] = {
    val impl = new CellImpl[K, V](pool, key, updater, init)
    pool.register(impl)
    impl
  }

  /**
   * Create a cell completer which is already completed with value `result`.
   *
   * Note: there is no `K` type parameter, since we always use type
   * `DefaultKey[V]`, no other key would make sense.
   */
  def completed[V](result: V)(implicit updater: Updater[V], pool: HandlerPool): CellCompleter[DefaultKey[V], V] = {
    val impl = new CellImpl[DefaultKey[V], V](pool, new DefaultKey[V], updater, _ => NoOutcome)
    pool.register(impl)
    impl.putFinal(result)
    impl
  }

}
