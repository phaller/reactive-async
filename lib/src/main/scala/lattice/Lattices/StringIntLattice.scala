package lattice

import cell._

class StringIntKey(s: String) extends Key[Int] {
	val lattice = new StringIntLattice

  def resolve[K <: Key[Int]](cells: Seq[Cell[K, Int]]): Seq[(Cell[K, Int], Int)] = {
    cells.map((cell: Cell[K, Int]) => (cell, 0))
  }
  def fallback[K <: Key[Int]](cells: Seq[Cell[K, Int]]): Seq[(Cell[K, Int], Int)] = {
    cells.map((cell: Cell[K, Int]) => (cell, 1))
  }

  override def toString = s
}

object StringIntKey {
  implicit def strToIntKey(s: String): StringIntKey =
    new StringIntKey(s)
}

class StringIntLattice extends Lattice[Int] {
	override def join(current: Int, next: Int): Int = {
      if(current != next) next
      else current
  }

  override def empty: Int = 0
}
