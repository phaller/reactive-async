package cell

import scala.util.Try

import lattice.{Lattice, Key, DefaultKey}


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
  def put(x: V, isFinal: Boolean): Unit

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

  /**
   * Create a cell completer which is already completed with value `result`.
   *
   * Note: there is no `K` type parameter, since we always use type
   * `DefaultKey[V]`, no other key would make sense.
   */
  def completed[V](pool: HandlerPool, result: V)(implicit lattice: Lattice[V]): CellCompleter[DefaultKey[V], V] = {
    val impl = new CellImpl[DefaultKey[V], V](pool, new DefaultKey[V], lattice)
    pool.register(impl)
    impl.putFinal(result)
    impl
  }

}
