package com.phaller.rasync
package test

import lattice._

object PurityKey extends Key[Purity] {

  def resolve[K <: Key[Purity]](cells: Iterable[Cell[K, Purity]]): Iterable[(Cell[K, Purity], Purity)] = {
    cells.map(cell => (cell, Pure))
  }

  def fallback[K <: Key[Purity]](cells: Iterable[Cell[K, Purity]]): Iterable[(Cell[K, Purity], Purity)] = {
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

  //  implicit object PurityUpdater extends MonotonicUpdater[Purity] {
  //    override def lteq(v1: Purity, v2: Purity): Boolean = {
  //      if (v1 == UnknownPurity) true
  //      else if (v1 == v2) true
  //      else false
  //    }
  //
  //    override val bottom: Purity = UnknownPurity
  //  }
}
