package lattice

import cell._

class StringIntKey(s: String) extends Key[Int] {
	val lattice = new StringIntLattice

  def resolve[K <: Key[Int]](cells: Seq[Cell[K, Int]]): Seq[Option[(Cell[K, Int], Int)]] = {
    cells.map((cell: Cell[K, Int]) => Some((cell, 0)))
  }
  def default[K <: Key[Int]](cells: Seq[Cell[K, Int]]): Seq[Option[(Cell[K, Int], Int)]] = {
    cells.map((cell: Cell[K, Int]) => Some((cell, 1)))
  }

  override def toString = s
}

object StringIntKey {
  implicit def strToIntKey(s: String): StringIntKey =
    new StringIntKey(s)
}

class StringIntLattice extends Lattice[Int] {
	override def join(current: Int, next: Int): Option[Int] = {
      if(current != next) Some(next)
      else None
  }

  override def empty: Int = 0
}
