package opal

import cell._
import lattice.{ Lattice, Key }

object ImmutabilityKey extends Key[Immutability] {

  def resolve[K <: Key[Immutability]](cells: Seq[Cell[K, Immutability]]): Seq[(Cell[K, Immutability], Immutability)] = {
    val conditionallyImmutableCells = cells.filter(_.getResult() == ConditionallyImmutable)
    if (conditionallyImmutableCells.nonEmpty)
      cells.map(cell => (cell, ConditionallyImmutable))
    else
      cells.map(cell => (cell, Immutable))
  }
  def fallback[K <: Key[Immutability]](cells: Seq[Cell[K, Immutability]]): Seq[(Cell[K, Immutability], Immutability)] = {
    val conditionallyImmutableCells = cells.filter(_.getResult() == ConditionallyImmutable)
    if (conditionallyImmutableCells.nonEmpty)
      conditionallyImmutableCells.map(cell => (cell, cell.getResult()))
    else
      cells.map(cell => (cell, Immutable))
  }

  override def toString = "Immutability"
}

sealed trait Immutability
case object Mutable extends Immutability
case object ConditionallyImmutable extends Immutability
case object Immutable extends Immutability

object Immutability {

  implicit object ImmutabilityLattice extends Lattice[Immutability] {
    override def join(current: Immutability, next: Immutability): Immutability = {
      if (<=(next, current)) current
      else next
    }

    def <=(lhs: Immutability, rhs: Immutability): Boolean = {
      lhs == rhs || lhs == Immutable ||
        (lhs == ConditionallyImmutable && rhs != Immutable)
    }

    override def empty: Immutability = Immutable
  }

}
