package com.phaller.rasync
package lattice

import scala.annotation.implicitNotFound

trait PartialOrderingWithBottom[V] extends PartialOrdering[V] {
  /**
   * Result of comparing x with operand y. Returns None if operands are not comparable. If operands are comparable, returns Some(r) where
   * r < 0 iff x < y
   * r == 0 iff x == y
   * r > 0 iff x > y
   */
  override def tryCompare(x: V, y: V): Option[Int] =
    if (lt(x, y)) Some(-1)
    else if (gt(x, y)) Some(1)
    else if (equiv(x, y)) Some(0)
    else None

  val bottom: V
}

object PartialOrderingWithBottom {
  def trivial[T >: Null]: PartialOrderingWithBottom[T] = {
    new PartialOrderingWithBottom[T] {
      override val bottom: T = null
      override def lteq(v1: T, v2: T): Boolean =
        (v1 == bottom) || (v1 == v2)
    }
  }
}

@implicitNotFound("type ${V} does not have a Lattice instance")
trait Lattice[V] extends PartialOrderingWithBottom[V] {
  /**
   * Return the join of v1 and v2 wrt. the lattice.
   */
  def join(v1: V, v2: V): V

  def lteq(v1: V, v2: V): Boolean = {
    join(v1, v2) == v2
  }

  override def gteq(v1: V, v2: V): Boolean = {
    join(v1, v2) == v1
  }
}

object Lattice {
  implicit def pair[T](implicit lattice: Lattice[T]): Lattice[(T, T)] = {
    new Lattice[(T, T)] {
      def join(v1: (T, T), v2: (T, T)): (T, T) =
        (lattice.join(v1._1, v2._1), lattice.join(v1._2, v2._2))
      val bottom: (T, T) =
        (lattice.bottom, lattice.bottom)
      override def lteq(v1: (T, T), v2: (T, T)): Boolean =
        lattice.lteq(v1._1, v2._1) && lattice.lteq(v1._2, v2._2)
      override def gteq(v1: (T, T), v2: (T, T)): Boolean =
        lattice.gteq(v1._1, v2._1) && lattice.gteq(v1._2, v2._2)
    }
  }
}
