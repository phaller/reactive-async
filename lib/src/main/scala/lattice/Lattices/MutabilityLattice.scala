package lattice

import cell._

object MutabilityKey extends Key[Mutability] {
	val lattice = new MutabilityLattice

  def resolve[K <: Key[Mutability]](cells: Seq[Cell[K, Mutability]]): Seq[Option[(Cell[K, Mutability], Mutability)]] = {
    cells.map((cell: Cell[K, Mutability]) => Some((cell, Immutable)))
  }
  def default[K <: Key[Mutability]](cells: Seq[Cell[K, Mutability]]): Seq[Option[(Cell[K, Mutability], Mutability)]] = {
    cells.map((cell: Cell[K, Mutability]) => Some((cell, Mutable)))
  }

  override def toString = "Mutability"
}

sealed trait Mutability
case object UnknownMutability extends Mutability
case object Mutable extends Mutability
case object ConditionallyImmutable extends Mutability
case object Immutable extends Mutability

class MutabilityLattice extends Lattice[Mutability] {
	override def join(current: Mutability, next: Mutability): Option[Mutability] = {
    if(current == UnknownMutability || <(current, next)) Some(next)
    else if(current == next) None
    else throw LatticeViolationException(current, next)
  }

  def <(current: Mutability, next: Mutability): Boolean = {
    if(current == ConditionallyImmutable && next != ConditionallyImmutable) true
    else false
  }

  override def empty: Mutability = UnknownMutability
}
