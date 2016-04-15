package lattice

import cell._

object PurenessKey extends Key[Purity] {
	val lattice = new PurityLattice

  def resolve[K <: Key[Purity]](cells: Seq[Cell[K, Purity]]): Seq[Option[(Cell[K, Purity], Purity)]] = {
    cells.map((cell: Cell[K, Purity]) => Some((cell, Pure)))
  }
  def default[K <: Key[Purity]](cells: Seq[Cell[K, Purity]]): Seq[Option[(Cell[K, Purity], Purity)]] = {
    cells.map((cell: Cell[K, Purity]) => Some((cell, Pure)))
  }

  override def toString = "Pureness"
}

sealed trait Purity
case object UnknownPurity extends Purity
case object Pure extends Purity
case object Impure extends Purity

class PurityLattice extends Lattice[Purity] {
	override def join(current: Purity, next: Purity): Option[Purity] = {
    if(current == UnknownPurity) Some(next)
    else if(current == next) None
    else throw LatticeViolationException(current, next)
  }

  override def empty: Purity = UnknownPurity
}
