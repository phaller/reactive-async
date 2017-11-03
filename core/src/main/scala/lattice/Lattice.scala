package lattice

import scala.annotation.implicitNotFound

@implicitNotFound("type ${V} does not have a Lattice instance")
trait Lattice[V] {
  /**
   * Return Some(v) if a new value v was computed (v != current), else None.
   * If it fails, throw exception.
   */
  def join(current: V, next: V): V
  def empty: V
}

object Lattice {

  implicit def pair[T](implicit lattice: Lattice[T]): Lattice[(T, T)] = {
    new Lattice[(T, T)] {
      def join(current: (T, T), next: (T, T)): (T, T) =
        (lattice.join(current._1, next._1), lattice.join(current._2, next._2))
      def empty: (T, T) =
        (lattice.empty, lattice.empty)
    }
  }

  def trivial[T >: Null]: Lattice[T] = {
    new Lattice[T] {
      override def empty: T = null
      override def join(current: T, next: T): T = {
        if (current == null) next
        else throw LatticeViolationException(current, next)
      }
    }
  }

}

final case class LatticeViolationException[D](current: D, next: D) extends IllegalStateException(
  s"Violation of lattice with current $current and next $next!")
