package com.phaller.rasync.test.lattice

import com.phaller.rasync.Cell
import com.phaller.rasync.lattice.{ Key, Lattice }

object ImmutabilityKey extends Key[Immutability] {

  def resolve[K <: Key[Immutability]](cells: Iterable[Cell[K, Immutability]]): Iterable[(Cell[K, Immutability], Immutability)] = {
    val conditionallyImmutableCells = cells.filter(_.getResult() == ConditionallyImmutable)
    if (conditionallyImmutableCells.nonEmpty)
      cells.map(cell => (cell, ConditionallyImmutable))
    else
      cells.map(cell => (cell, Immutable))
  }
  def fallback[K <: Key[Immutability]](cells: Iterable[Cell[K, Immutability]]): Iterable[(Cell[K, Immutability], Immutability)] = {
    cells.map(cell => (cell, Immutable))
  }

  override def toString = "Immutability"
}

sealed trait Immutability
case object Mutable extends Immutability
case object ConditionallyImmutable extends Immutability
case object Immutable extends Immutability

object Immutability {

  implicit object ImmutabilityLattice extends Lattice[Immutability] {
    override def join(v1: Immutability, v2: Immutability): Immutability = {
      if (lteq(v2, v1)) v1
      else v2
    }

    override def lteq(lhs: Immutability, rhs: Immutability): Boolean = {
      lhs == rhs || lhs == Immutable ||
        (lhs == ConditionallyImmutable && rhs != Immutable)
    }

    override val bottom: Immutability = Immutable
  }
}
