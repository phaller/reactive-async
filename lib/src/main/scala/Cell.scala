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

  def onCycle(callback: Seq[K] => Unit)
}
