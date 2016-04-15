package lattice

import scala.annotation.implicitNotFound

import cell._

trait Key[V] {
  val lattice: Lattice[V]

  def resolve[K <: Key[V]](cells: Seq[Cell[K, V]]): Seq[Option[(Cell[K, V], V)]]
  def default[K <: Key[V]](cells: Seq[Cell[K, V]]): Seq[Option[(Cell[K, V], V)]]
}

@implicitNotFound("type $V does not have a Lattice instance")
trait Lattice[V] {
  /** Return Some(v) if a new value v was computed (v != current), else None.
    * If it fails, throw exception.
    */
  def join(current: V, next: V): Option[V]
  def empty: V
}
