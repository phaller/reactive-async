package lattice

class StringIntKey(s: String) extends Key[Int] {
  def resolve: Int = 0
	def lattice = new StringIntLattice
  override def toString = s
}

object StringIntKey {
  implicit def strToIntKey(s: String): StringIntKey =
    new StringIntKey(s)
}

class StringIntLattice extends Lattice[Int] {
	override def join(current: Option[Int], next: Int): Option[Int] = current match {
    case Some(c) =>
      if(c != next) throw LatticeViolationException(current, next)
      else None
    case _ => Some(next)
  }
}
