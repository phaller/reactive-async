package com.phaller.rasync
package lattice

import scala.language.implicitConversions

class StringIntKey(s: String) extends Key[Int] {
  def resolve[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] = {
    cells.map((cell: Cell[K, Int]) => (cell, 0))
  }

  def fallback[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] = {
    cells.map((cell: Cell[K, Int]) => (cell, 1))
  }

  override def toString = s
}

object StringIntKey {
  implicit def strToIntKey(s: String): StringIntKey =
    new StringIntKey(s)
}

class StringIntUpdater extends Updater[Int] with PartialOrderingWithBottom[Int] {
  override def update(v1: Int, v2: Int): Int =
    if (v1 != v2) v2
    else v1

  override def bottom: Int = 0

  override def lteq(x: Int, y: Int): Boolean = x <= y
}

