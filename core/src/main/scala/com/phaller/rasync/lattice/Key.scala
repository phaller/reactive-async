package com.phaller.rasync
package lattice

import com.phaller.rasync.cell.Cell

trait Key[V] {
  def resolve(cells: Iterable[Cell[V]]): Iterable[(Cell[V], V)]
  def fallback(cells: Iterable[Cell[V]]): Iterable[(Cell[V], V)]
}

class DefaultKey[V] extends Key[V] {

  def resolve(cells: Iterable[Cell[V]]): Iterable[(Cell[V], V)] = {
    cells.map(cell => (cell, cell.getResult()))
  }

  def fallback(cells: Iterable[Cell[V]]): Iterable[(Cell[V], V)] = {
    cells.map(cell => (cell, cell.getResult()))
  }

}
