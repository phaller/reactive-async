import cell.Cell
import lattice.Lattice
import lattice.Key

object NaturalNumberKey extends Key[Int] {
  val lattice = new NaturalNumberLattice

  def resolve[K <: Key[Int]](cells: Seq[Cell[K, Int]]): Seq[(Cell[K, Int], Int)] = {
    cells.map(cell => (cell, cell.getResult()))
  }
  def default[K <: Key[Int]](cells: Seq[Cell[K, Int]]): Seq[(Cell[K, Int], Int)] = {
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
