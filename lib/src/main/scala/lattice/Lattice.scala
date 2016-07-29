package lattice

import scala.annotation.implicitNotFound

import cell._


@implicitNotFound("type $V does not have a Lattice instance")
trait Lattice[V] {
  /** Return Some(v) if a new value v was computed (v != current), else None.
    * If it fails, throw exception.
    */
  def join(current: V, next: V): V
  def empty: V
}

final case class LatticeViolationException[D](current: D, next: D) extends IllegalStateException(
  s"Violation of lattice with current $current and next $next!"
)

trait Key[V] {
  val lattice: Lattice[V]

  def resolve[K <: Key[V]](cells: Seq[Cell[K, V]]): Seq[(Cell[K, V], V)]
  def fallback[K <: Key[V]](cells: Seq[Cell[K, V]]): Seq[(Cell[K, V], V)]
}
