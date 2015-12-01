
import java.util.concurrent.atomic._

/**
 * Example:
 *
 *   val barRetTypeCell: Cell[(Entity, PropertyKind), ObjectType]
 */
trait Cell[K, V] {
  def key: K
  def property: V

  def dependencies: Seq[K]
  def addDependency(other: K)
  def removeDependency(other: K)

  // sobald sich der Wert dieser Cell ändert, müssen die dependee Cells benachrichtigt werden
  def dependees: Seq[K]
  def addDependee(k: K): Unit
  def removeDependee(k: K): Unit

  def putFinal(x: V): Unit
  def putNext(x: V): Unit

  // internal API
  def onNextResult(callback: V => Unit)

  /**
   * Adds a dependency on some `other` cell.
   *
   * Example:
   *   whenComplete(cell, x => !x, Impure) // if `cell` is completed and the predicate is true (meaning
   *                                       // `cell` is impure), `this` cell can be completed with constant `Impure`
   *
   * @param other  Cell that `this` Cell depends on.
   * @param pred   Predicate used to decide whether a final result of `this` Cell can be computed early.
   * @param value  Early result value.
   */
  def whenComplete(other: Cell[K, V], pred: V => Boolean, value: V): Unit

  /**
   * Registers a call-back function to be invoked when quiescence is reached, but `this` cell has not been
   * completed, yet. The call-back function is passed a sequence of the cells that `this` cell depends on.
   */
  def onCycle(callback: Seq[Cell[K, V]] => V)

}


trait CellCompleter[K, V] {
  def cell: Cell[K, V]

  def putFinal(x: V): Unit
  def putNext(x: V): Unit
}

object CellCompleter {
  def apply[K, V](): CellCompleter[K, V] = {
    ???
  }
}


abstract class CellImpl[K, V] extends Cell[K, V] {

  private val state = new AtomicReference[AnyRef]()

}
