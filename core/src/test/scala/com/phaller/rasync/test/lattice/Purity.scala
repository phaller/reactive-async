package com.phaller.rasync
package test

import com.phaller.rasync.cell.Cell
import com.phaller.rasync.lattice._

object PurityKey extends Key[Purity, Null] {

  def resolve(cells: Iterable[Cell[Purity, Null]]): Iterable[(Cell[Purity, Null], Purity)] = {
    cells.map(cell => (cell, Pure))
  }

  def fallback(cells: Iterable[Cell[Purity, Null]]): Iterable[(Cell[Purity, Null], Purity)] = {
    cells.map(cell => (cell, Pure))
  }

  override def toString = "Purity"
}

sealed trait Purity
case object UnknownPurity extends Purity
case object Pure extends Purity
case object Impure extends Purity

object Purity {
  implicit object PurityOrdering extends PartialOrderingWithBottom[Purity] {
    override def lteq(v1: Purity, v2: Purity): Boolean = {
      if (v1 == UnknownPurity) true
      else if (v1 == v2) true
      else false
    }

    override val bottom: Purity = UnknownPurity
  }
}
