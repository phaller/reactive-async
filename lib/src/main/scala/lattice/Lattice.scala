package lattice

import scala.annotation.implicitNotFound

trait Key[V] {
  def resolve: V
  def lattice: Lattice[V]
}

@implicitNotFound("type $V does not have a Lattice instance")
trait Lattice[V] {
  /** Return Some(v) if a new value v was computed (v != current), else None.
    * If it fails, throw exception.
    */
  def join(current: Option[V], next: V): Option[V]
}

object Lattice {
  implicit val purityLattice = new PurityLattice
  implicit val strIntLattice = new StringIntLattice
  implicit val mutabilityLattice = new MutabilityLattice
}
