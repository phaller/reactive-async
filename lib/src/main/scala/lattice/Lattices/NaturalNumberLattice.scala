package lattice

import cell.Cell

object NaturalNumberKey extends Key[Int] {
  def resolve[K <: Key[Int]](cells: Seq[Cell[K, Int]]): Seq[(Cell[K, Int], Int)] = {
    cells.map(cell => (cell, cell.getResult()))
  }
  def fallback[K <: Key[Int]](cells: Seq[Cell[K, Int]]): Seq[(Cell[K, Int], Int)] = {
    cells.map(cell => (cell, cell.getResult()))
  }
}

class NaturalNumberLattice extends Lattice[Int] {
  override def join(current: Int, next: Int): Int = {
    if (next > current) next
    else current
  }

  override def empty: Int = 0
}
