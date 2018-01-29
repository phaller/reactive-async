package lattice

import scala.annotation.implicitNotFound

@implicitNotFound("type ${V} does not have a Lattice instance")
trait Lattice[V] extends PartialOrdering[V] {
  /**
   * Return Some(v) if a new value v was computed (v != current), else None.
   * If it fails, throw exception.
   */
  def join(current: V, next: V): V

  def lteq(v1: V, v2: V): Boolean = {
    try
      join(v1, v2) == v2
    catch { case _: LatticeViolationException[V] => false }
  }

  override def gteq(v1: V, v2: V): Boolean = {
    try
      join(v1, v2) == v1
    catch { case _: LatticeViolationException[V] => false }
  }

  /**
   * Result of comparing x with operand y. Returns None if operands are not comparable. If operands are comparable, returns Some(r) where
   * r < 0 iff x < y
   * r == 0 iff x == y
   * r > 0 iff x > y
   */
  override def tryCompare(x: V, y: V): Option[Int] =
    if (lt(x, y)) Some(-1)
    else if (gt(x, y)) Some(1)
    else if (x == y) Some(0)
    else None

  def empty: V
}

object Lattice {

  implicit def pair[T](implicit lattice: Lattice[T]): Lattice[(T, T)] = {
    new Lattice[(T, T)] {
      def join(current: (T, T), next: (T, T)): (T, T) =
        (lattice.join(current._1, next._1), lattice.join(current._2, next._2))
      def empty: (T, T) =
        (lattice.empty, lattice.empty)
      override def lteq(v1: (T, T), v2: (T, T)): Boolean =
        lattice.lteq(v1._1, v2._1) && lattice.lteq(v1._2, v2._2)
      override def gteq(v1: (T, T), v2: (T, T)): Boolean =
        lattice.gteq(v1._1, v2._1) && lattice.gteq(v1._2, v2._2)
    }
  }

  def trivial[T >: Null]: Lattice[T] = {
    new Lattice[T] {
      override def empty: T = null
      override def join(current: T, next: T): T = {
        if (current == null) next
        else throw LatticeViolationException(current, next)
      }
      override def lteq(v1: T, v2: T): Boolean =
        (v1 == empty) || (v1 == v2)
      override def gteq(v1: T, v2: T): Boolean =
        (v2 == empty) || (v1 == v2)
    }
  }

}

final case class LatticeViolationException[D](current: D, next: D) extends IllegalStateException(
  s"Violation of lattice with current $current and next $next!")
