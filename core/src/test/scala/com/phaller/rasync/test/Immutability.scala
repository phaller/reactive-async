package com.phaller.rasync
package test

import lattice.{ MonotonicUpdater, Key, Lattice }

object ImmutabilityKey extends Key[Immutability] {

  def resolve[K <: Key[Immutability]](cells: Iterable[Cell[K, Immutability]]): Iterable[(Cell[K, Immutability], Immutability)] = {
    val conditionallyImmutableCells = cells.filter(_.getResult() == ConditionallyImmutable)
    if (conditionallyImmutableCells.nonEmpty)
      cells.map(cell => (cell, ConditionallyImmutable))
    else
      cells.map(cell => (cell, Immutable))
  }
  def fallback[K <: Key[Immutability]](cells: Iterable[Cell[K, Immutability]]): Iterable[(Cell[K, Immutability], Immutability)] = {
    val conditionallyImmutableCells = cells.filter(_.getResult() == ConditionallyImmutable)
    if (conditionallyImmutableCells.nonEmpty)
      conditionallyImmutableCells.map(cell => (cell, cell.getResult()))
    else
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

    override def bottom: Immutability = Immutable
  }

  implicit object ImmutabilityUpdater extends MonotonicUpdater[Immutability] {
    override def update(v1: Immutability, v2: Immutability): Immutability = {
      if (lteq(v2, v1)) v1
      else v2
    }

    def lteq(lhs: Immutability, rhs: Immutability): Boolean = {
      lhs == rhs || lhs == Immutable ||
        (lhs == ConditionallyImmutable && rhs != Immutable)
    }

    override def bottom: Immutability = Immutable
  }

}
