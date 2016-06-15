package lattice

import cell._

object PurenessKey extends Key[Purity] {

  val lattice = new PurityLattice

  def resolve[K <: Key[Purity]](cells: Seq[Cell[K, Purity]]): Seq[(Cell[K, Purity], Purity)] = {
    cells.map(cell => (cell, Pure))
  }

  def default[K <: Key[Purity]](cells: Seq[Cell[K, Purity]]): Seq[(Cell[K, Purity], Purity)] = {
    cells.map(cell => (cell, Pure))
  }

  override def toString = "Pureness"
}

sealed trait Purity
case object UnknownPurity extends Purity
case object Pure extends Purity
case object Impure extends Purity

class PurityLattice extends Lattice[Purity] {

  override def join(current: Purity, next: Purity): Purity = {
    if (current == UnknownPurity) next
    else if (current == next) current
    else throw LatticeViolationException(current, next)
  }

  override val empty: Purity = UnknownPurity

}
