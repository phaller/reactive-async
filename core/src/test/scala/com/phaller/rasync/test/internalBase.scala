package com.phaller.rasync
package test

import org.scalatest.FunSuite
import com.phaller.rasync.lattice.Updater
import lattice.{ StringIntKey, StringIntUpdater }

class InternalBaseSuite extends FunSuite {

  implicit val stringIntUpdater: Updater[Int] = new StringIntUpdater

  test("cellDependencies: By adding dependencies") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("key1")
    val completer2 = CellCompleter[StringIntKey, Int]("key2")
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.whenComplete(cell2, x => if (x == 0) FinalOutcome(0) else NoOutcome)
    cell1.whenComplete(cell2, x => if (x == 0) FinalOutcome(0) else NoOutcome)

    assert(cell1.numCompleteDependencies == 1)
    assert(cell2.numCompleteDependencies == 0)
  }

  test("cellDependencies: By removing dependencies") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("key1")
    val completer2 = CellCompleter[StringIntKey, Int]("key2")
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.whenComplete(cell2, x => if (x == 0) FinalOutcome(0) else NoOutcome)
    cell1.whenComplete(cell2, x => if (x == 0) FinalOutcome(0) else NoOutcome)

    completer1.putFinal(0)

    assert(cell1.numCompleteDependencies == 0)
    assert(cell2.numCompleteDependencies == 0)
  }
}
