package lattice

import cell.Cell

trait Key[V] {
  def resolve[K <: Key[V]](cells: Seq[Cell[K, V]]): Seq[(Cell[K, V], V)]
  def fallback[K <: Key[V]](cells: Seq[Cell[K, V]]): Seq[(Cell[K, V], V)]
}

class DefaultKey[V] extends Key[V] {

  def resolve[K <: Key[V]](cells: Seq[Cell[K, V]]): Seq[(Cell[K, V], V)] = {
    cells.map(cell => (cell, cell.getResult()))
  }

  def fallback[K <: Key[V]](cells: Seq[Cell[K, V]]): Seq[(Cell[K, V], V)] = {
    cells.map(cell => (cell, cell.getResult()))
  }

}
