package lattice

import cell._

object PurenessKey extends Key[Purity] {
  def resolve[K <: Key[Purity]](cells: Seq[Cell[K, Purity]]): Seq[Option[(Cell[K, Purity], Purity)]] = {
    cells.map((cell: Cell[K, Purity]) => Some((cell, Pure)))
  }
  def default[K <: Key[Purity]](cells: Seq[Cell[K, Purity]]): Seq[Option[(Cell[K, Purity], Purity)]] = {
    cells.map((cell: Cell[K, Purity]) => Some((cell, Pure)))
  }
  def lattice = new PurityLattice
  override def toString = "Pureness"
}

sealed trait Purity
case object Pure extends Purity
case object Impure extends Purity

class PurityLattice extends Lattice[Purity] {
	override def join(current: Option[Purity], next: Purity): Option[Purity] = current match {
    case Some(c) =>
        if(c != next) throw LatticeViolationException(current, next)
        else None
    case _ => Some(next)
  }
}
