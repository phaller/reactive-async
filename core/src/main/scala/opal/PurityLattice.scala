package opal

import cell._
import lattice.{ Lattice, LatticeViolationException, Key }

object PurityKey extends Key[Purity] {

  def resolve[K <: Key[Purity]](cells: Seq[Cell[K, Purity]]): Seq[(Cell[K, Purity], Purity)] = {
    cells.map(cell => (cell, Pure))
  }

  def fallback[K <: Key[Purity]](cells: Seq[Cell[K, Purity]]): Seq[(Cell[K, Purity], Purity)] = {
    cells.map(cell => (cell, Pure))
  }

  override def toString = "Purity"
}

sealed trait Purity
case object UnknownPurity extends Purity
case object Pure extends Purity
case object Impure extends Purity

object Purity {

  implicit object PurityLattice extends Lattice[Purity] {
    override def join(current: Purity, next: Purity): Purity = {
      if (current == UnknownPurity) next
      else if (current == next) current
      else throw LatticeViolationException(current, next)
    }

    override val empty: Purity = UnknownPurity
  }

}
