package com.phaller.rasync
package lattice

/**
 * An updater defines, how to react to a value that is being put to a cell.
 * Given a `current` value of the cell a a `next` value, the `update` method
 * returns the new value of the cell of a "combination" of `current` and `next`.
 *
 * An updater also defines, what the initial value of a cell is.
 */
trait Updater[V] {
  val initial: V
  def update(current: V, next: V): V
}

/**
 * AggregationUpdaters are built on lattices and compute new values as `join` of
 * all incoming ("next") values. Therefore, AggregationUpdaters do not throw exceptions
 * as the `join` of two lattice values is always defined.
 *
 * The initial value is the bottom value of the lattice.
 */
class AggregationUpdater[V](val lattice: Lattice[V]) extends Updater[V] {
  override val initial: V = lattice.bottom
  override def update(current: V, next: V): V = lattice.join(current, next)
}

/**
 * MonotonicUpdaters are built on partial orderings. The incoming ("next") value is
 * used as the new value for the cell, as long as the update is monotonic.
 * Otherwise a NotMonotonicException is thrown.
 *
 * The initial value is the bottom value of the partial ordering.
 */
class MonotonicUpdater[V](val partialOrderingWithBottom: PartialOrderingWithBottom[V]) extends Updater[V] {
  override val initial: V = partialOrderingWithBottom.bottom

  override def update(current: V, next: V): V =
    if (partialOrderingWithBottom.lteq(current, next)) next
    else throw NotMonotonicException(current, next)
}

object Updater {
  // (implicitely) convert a lattice to its canonic alaggregation updater
  implicit def latticeToUpdater[T](implicit lattice: Lattice[T]): Updater[T] =
    new AggregationUpdater[T](lattice)

  // convert a lattice to its canonical monotonic updater
  def partialOrderingToUpdater[T](implicit partialOrderingWithBottom: PartialOrderingWithBottom[T]): Updater[T] =
    new MonotonicUpdater[T](partialOrderingWithBottom)

  // create an updater for pairs of values.
  def pair[T](implicit updater: Updater[T]): Updater[(T, T)] = new Updater[(T, T)] {
    override def update(current: (T, T), next: (T, T)): (T, T) =
      (updater.update(current._1, next._1), updater.update(current._2, next._2))

    override val initial: (T, T) =
      (updater.asInstanceOf[PartialOrderingWithBottom[T]].bottom, updater.asInstanceOf[PartialOrderingWithBottom[T]].bottom)
  }
}

final case class NotMonotonicException[D](current: D, next: D) extends IllegalStateException(
  s"Violation of ordering with current $current and next $next!")
