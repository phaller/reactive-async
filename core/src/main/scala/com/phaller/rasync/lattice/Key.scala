package com.phaller.rasync
package lattice

trait Key[V] {
  def resolve[K <: Key[V]](cells: Iterable[Cell[K, V]]): Iterable[(Cell[K, V], V)]
  def fallback[K <: Key[V]](cells: Iterable[Cell[K, V]]): Iterable[(Cell[K, V], V)]
}

class DefaultKey[V] extends Key[V] {

  def resolve[K <: Key[V]](cells: Iterable[Cell[K, V]]): Iterable[(Cell[K, V], V)] = {
    cells.map(cell => (cell, cell.getResult()))
  }

  def fallback[K <: Key[V]](cells: Iterable[Cell[K, V]]): Iterable[(Cell[K, V], V)] = {
    cells.map(cell => (cell, cell.getResult()))
  }

}
