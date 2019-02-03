package com.phaller.rasync
package lattice

import com.phaller.rasync.cell.Cell

trait Key[V, E >: Null] {
  def resolve(cells: Iterable[Cell[V, E]]): Iterable[(Cell[V, E], V)]
  def fallback(cells: Iterable[Cell[V, E]]): Iterable[(Cell[V, E], V)]
}

class DefaultKey[V, E >: Null] extends Key[V, E] {

  def resolve(cells: Iterable[Cell[V, E]]): Iterable[(Cell[V, E], V)] = {
    cells.map(cell => (cell, cell.getResult()))
  }

  def fallback(cells: Iterable[Cell[V, E]]): Iterable[(Cell[V, E], V)] = {
    cells.map(cell => (cell, cell.getResult()))
  }

}

object DefaultKey {
  def apply[V]: DefaultKey[V, Null] = new DefaultKey[V, Null]()
}
