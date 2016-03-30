package lattice

object PurenessKey extends Key[Purity] {
	def resolve: Purity = Pure
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
