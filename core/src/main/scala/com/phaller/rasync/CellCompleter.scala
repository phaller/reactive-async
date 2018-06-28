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

  def putFinal(x: V): Unit
  def putNext(x: V): Unit
  def put(x: V, isFinal: Boolean): Unit

  private[rasync] def tryNewState(value: V): Boolean

  /**
   * Put a final value but do not call callbacks targeting cells in `removeCallback` list.
   * This is list used, if cycles get resolved and no value should be propagated inside the cycle.
   *
   * @param value The value to put.
   * @param removeCallbacks A list of cells that do not get called.
   * @return true, iff the operation succeeded.
   */
  /* This method could be private[rasync] but is used in a benchmark. */
  def tryComplete(value: Try[V], removeCallbacks: Option[Iterable[Cell[K, V]]] = None): Boolean

  private[rasync] def removeDep(cell: Cell[K, V]): Unit
  private[rasync] def removeNextDep(cell: Cell[K, V]): Unit
}

object CellCompleter {

  /**
   * Create a completer for a cell holding values of type `V`
   * given a `HandlerPool` and a `Key[V]`.
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
