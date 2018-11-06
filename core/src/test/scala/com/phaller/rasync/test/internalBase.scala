package com.phaller.rasync
package test

import cell.{ CellCompleter, FinalOutcome, NoOutcome }
import com.phaller.rasync.lattice.Updater
import org.scalatest.FunSuite
import pool.HandlerPool
import lattice.IntUpdater

class InternalBaseSuite extends FunSuite {

  implicit val stringIntUpdater: Updater[Int] = new IntUpdater

  test("cellDependencies: By adding dependencies") {
    implicit val pool = new HandlerPool[Int]
    val completer1 = CellCompleter[Int]()
    val completer2 = CellCompleter[Int]()
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.when((_, x) => if (x.get.value == 0) FinalOutcome(0) else NoOutcome, cell2)
    cell1.when((_, x) => if (x.get.value == 0) FinalOutcome(0) else NoOutcome, cell2)

    assert(cell1.numDependencies == 1)
    assert(cell2.numDependencies == 0)
  }

  test("cellDependencies: By removing dependencies") {
    implicit val pool = new HandlerPool[Int]
    val completer1 = CellCompleter[Int]()
    val completer2 = CellCompleter[Int]()
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.when((_, x) => if (x.get.value == 0) FinalOutcome(0) else NoOutcome, cell2)
    cell1.when((_, x) => if (x.get.value == 0) FinalOutcome(0) else NoOutcome, cell2)

    completer1.putFinal(0)

    pool.onQuiescent(() => {
      assert(cell1.numDependencies == 0)
      assert(cell2.numDependencies == 0)
    })
  }
}
