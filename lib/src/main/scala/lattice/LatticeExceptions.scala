package lattice

final case class LatticeViolationException[D](current: D, next: D) extends IllegalStateException(
  s"Violation of lattice with current $current and next $next!"
)
