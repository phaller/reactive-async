package com.phaller.rasync.test.lattice

import com.phaller.rasync.cell.Cell
import com.phaller.rasync.lattice.{ Key, Updater }

import scala.language.implicitConversions

class StringIntKey(s: String) extends Key[Int, Null] {
  def resolve(cells: Iterable[Cell[Int, Null]]): Iterable[(Cell[Int, Null], Int)] = {
    cells.map((cell: Cell[Int, Null]) => (cell, 0))
  }

  def fallback(cells: Iterable[Cell[Int, Null]]): Iterable[(Cell[Int, Null], Int)] = {
    cells.map((cell: Cell[Int, Null]) => (cell, 1))
  }

  override def toString = s
}

object StringIntKey {
  implicit def strToIntKey(s: String): StringIntKey =
    new StringIntKey(s)
}

class IntUpdater extends Updater[Int] {
  override def update(v1: Int, v2: Int): Int =
    if (v1 != v2) v2
    else v1

  override val bottom: Int = 0
}

