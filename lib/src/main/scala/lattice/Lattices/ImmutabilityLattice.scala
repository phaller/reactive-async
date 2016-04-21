package lattice

import cell._

object ImmutabilityKey extends Key[Immutability] {
	val lattice = new ImmutabilityLattice

  def resolve[K <: Key[Immutability]](cells: Seq[Cell[K, Immutability]]): Seq[Option[(Cell[K, Immutability], Immutability)]] = {
    val conditionallyImmutableCells = cells.filter(_.getResult() == ConditionallyImmutable)
    if (conditionallyImmutableCells.nonEmpty)
      Seq(Some(conditionallyImmutableCells.head, ConditionallyImmutable))
    else
      Seq(Some(cells.filter(_.getResult() == Immutable).head, Immutable))
  }
  def default[K <: Key[Immutability]](cells: Seq[Cell[K, Immutability]]): Seq[Option[(Cell[K, Immutability], Immutability)]] = {
    val defaultCells = cells.filter { _.totalDependencies.isEmpty }
    defaultCells.map { cell =>
      Some((cell, cell.getResult()))
    }
  }

  override def toString = "Immutability"
}

sealed trait Immutability
case object Mutable extends Immutability
case object ConditionallyImmutable extends Immutability
case object Immutable extends Immutability

class ImmutabilityLattice extends Lattice[Immutability] {
	override def join(current: Immutability, next: Immutability): Option[Immutability] = {
    if (current == next || current == Mutable || <(next, current)) None
    else Some(next)
  }

  def <(lhs: Immutability, rhs: Immutability): Boolean = {
    if (lhs == Immutable) true
    else if (lhs == ConditionallyImmutable && rhs != Immutable) true
    else false
  }

  override def empty: Immutability = Immutable
}
