package com.phaller.rasync
package test

import cell._
import com.phaller.rasync.lattice.Updater
import org.scalatest.FunSuite
import pool.HandlerPool
import lattice.IntUpdater

import scala.util.Try

class InternalBaseSuite extends FunSuite {

  implicit val stringIntUpdater: Updater[Int] = new IntUpdater

  def if10thenFinal20(updates: Iterable[(Cell[Int], Try[ValueOutcome[Int]])]): Outcome[Int] =
    ifXthenFinalY(10, 20)(updates)

  def ifXthenFinalY(x: Int, y: Int)(upd: Iterable[(Cell[Int], Try[ValueOutcome[Int]])]): Outcome[Int] = {
    val c = upd.head._2
    if (c.get.value == x) FinalOutcome(y) else NoOutcome
  }

  test("cellDependencies: By adding dependencies") {
    implicit val pool = new HandlerPool[Int]
    val completer1 = CellCompleter[Int]()
    val completer2 = CellCompleter[Int]()
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.when(cell2)(if10thenFinal20)
    cell1.when(cell2)(if10thenFinal20)

    assert(cell1.numDependencies == 1)
    assert(cell2.numDependencies == 0)
  }

  test("cellDependencies: By removing dependencies") {
    implicit val pool = new HandlerPool[Int]
    val completer1 = CellCompleter[Int]()
    val completer2 = CellCompleter[Int]()
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.when(cell2)(if10thenFinal20)
    cell1.when(cell2)(if10thenFinal20)

    completer1.putFinal(0)

    pool.onQuiescent(() => {
      assert(cell1.numDependencies == 0)
      assert(cell2.numDependencies == 0)
    })
  }
}
