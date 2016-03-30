package lattice

object MutabilityKey extends Key[Mutability] {
	def resolve: Mutability = Immutable
	def lattice = new MutabilityLattice
  override def toString = "Mutability"
}

sealed trait Mutability
case object Mutable extends Mutability
case object ConditionallyImmutable extends Mutability
case object Immutable extends Mutability

class MutabilityLattice extends Lattice[Mutability] {
	override def join(current: Option[Mutability], next: Mutability): Option[Mutability] = current match {
    case Some(c) =>
      if(<(c, next)) Some(next)
      else if(c == next) None
      else throw LatticeViolationException(current, next)
    case _ => Some(next)
  }

  def <(current: Mutability, next: Mutability): Boolean = {
    if(current == ConditionallyImmutable && next != ConditionallyImmutable) true
    else false
  }
}
