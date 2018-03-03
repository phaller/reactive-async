package lattice

trait Updater[V] {
  this: PartialOrderingWithBottom[V] =>

  def bottom: V
  def update(current: V, next: V): V
}

trait AggregationUpdater[V] extends Lattice[V] with Updater[V] {
  override def update(current: V, next: V): V = join(current, next)
}

trait MonotonicUpdater[V] extends PartialOrderingWithBottom[V] with Updater[V] {
  override def update(current: V, next: V): V =
    if (lteq(current, next)) next
    else throw new NotMonotonicException(current, next)
}

object Updater {
  implicit def latticeToUpdater[T](implicit lattice: Lattice[T]): Updater[T] =
    new AggregationUpdater[T] {
      override def join(v1: T, v2: T): T = lattice.join(v1, v2)

      override def bottom: T = lattice.bottom
    }

  def partialOrderingToUpdater[T](implicit partialOrderingWithBottom: PartialOrderingWithBottom[T]): Updater[T] =
    new MonotonicUpdater[T] {
      override def bottom: T = partialOrderingWithBottom.bottom
      override def lteq(x: T, y: T): Boolean = partialOrderingWithBottom.lteq(x, y)
    }

  //  def pair[T](implicit lattice: Lattice[T]): Updater[(T, T)] = latticeToUpdater(Lattice.pair(lattice))

  def pair[T](implicit updater: Updater[T]): Updater[(T, T)] = new Updater[(T, T)] with PartialOrderingWithBottom[(T, T)] {
    override def update(current: (T, T), next: (T, T)): (T, T) =
      (updater.update(current._1, next._1), updater.update(current._2, next._2))

    override def bottom: (T, T) =
      (updater.asInstanceOf[PartialOrderingWithBottom[T]].bottom, updater.asInstanceOf[PartialOrderingWithBottom[T]].bottom)

    override def lteq(x: (T, T), y: (T, T)): Boolean =
      updater.asInstanceOf[PartialOrderingWithBottom[T]].lteq(x._1, y._1) && updater.asInstanceOf[PartialOrderingWithBottom[T]].lteq(x._2, y._2)
  }
}

final case class NotMonotonicException[D](current: D, next: D) extends IllegalStateException(
  s"Violation of lattice with current $current and next $next!")
