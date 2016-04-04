package lattice

import cell._

object ImmutabilityKey extends Key[Immutability] {
	val lattice = new ImmutabilityLattice

  def resolve[K <: Key[Immutability]](cells: Seq[Cell[K, Immutability]]): Seq[Option[(Cell[K, Immutability], Immutability)]] = {
    cells.map((cell: Cell[K, Immutability]) => Some((cell, Immutable)))
  }
  def default[K <: Key[Immutability]](cells: Seq[Cell[K, Immutability]]): Seq[Option[(Cell[K, Immutability], Immutability)]] = {
    cells.map((cell: Cell[K, Immutability]) => Some((cell, Mutable)))
  }

  override def toString = "Immutability"
}

sealed trait Immutability
case object UnknownImmutability extends Immutability
case object Mutable extends Immutability
case object ConditionallyImmutable extends Immutability
case object Immutable extends Immutability

class ImmutabilityLattice extends Lattice[Immutability] {
	override def join(current: Immutability, next: Immutability): Option[Immutability] = {
    if(current == UnknownImmutability || <(current, next)) Some(next)
    else if(current == next) None
    else throw LatticeViolationException(current, next)
  }

  def <(current: Immutability, next: Immutability): Boolean = {
    if(current == ConditionallyImmutable && next == Immutable) true
    else false
  }

  override def empty: Immutability = UnknownImmutability
}
