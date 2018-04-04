package com.phaller.rasync
package lattice

trait Updater[V] {
  val initial: V
  def update(current: V, next: V): V
}

class AggregationUpdater[V](val lattice: Lattice[V]) extends Updater[V] {
  override val initial: V = lattice.bottom
  override def update(current: V, next: V): V = lattice.join(current, next)
}

class MonotonicUpdater[V](val partialOrderingWithBottom: PartialOrderingWithBottom[V]) extends Updater[V] {
  override val initial: V = partialOrderingWithBottom.bottom

  override def update(current: V, next: V): V =
    if (partialOrderingWithBottom.lteq(current, next)) next
    else throw NotMonotonicException(current, next)
}

object Updater {
  implicit def latticeToUpdater[T](implicit lattice: Lattice[T]): Updater[T] =
    new AggregationUpdater[T](lattice)

  def partialOrderingToUpdater[T](implicit partialOrderingWithBottom: PartialOrderingWithBottom[T]): Updater[T] =
    new MonotonicUpdater[T](partialOrderingWithBottom)

  //  def pair[T](implicit lattice: Lattice[T]): Updater[(T, T)] = latticeToUpdater(Lattice.pair(lattice))

  def pair[T](implicit updater: Updater[T]): Updater[(T, T)] = new Updater[(T, T)] {
    override def update(current: (T, T), next: (T, T)): (T, T) =
      (updater.update(current._1, next._1), updater.update(current._2, next._2))

    override val initial: (T, T) =
      (updater.asInstanceOf[PartialOrderingWithBottom[T]].bottom, updater.asInstanceOf[PartialOrderingWithBottom[T]].bottom)

    //    override val initial: (T, T) = bottom
    //
    //    override def lteq(x: (T, T), y: (T, T)): Boolean =
    //      updater.asInstanceOf[PartialOrderingWithBottom[T]].lteq(x._1, y._1) && updater.asInstanceOf[PartialOrderingWithBottom[T]].lteq(x._2, y._2)
  }
}

final case class NotMonotonicException[D](current: D, next: D) extends IllegalStateException(
  s"Violation of lattice with current $current and next $next!")
