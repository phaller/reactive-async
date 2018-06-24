package com.phaller.rasync
package test

import com.phaller.rasync.lattice.{ Key, Updater }
import lattice._
import org.scalatest.FunSuite

import scala.concurrent.duration._
import scala.concurrent.Await

class PsSuite extends FunSuite {

  implicit val stringIntUpdater: Updater[Int] = new StringIntUpdater

  test("cell dependency on itself whenNextSequential") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[ReactivePropertyStoreKey, Int](new ReactivePropertyStoreKey())
    val cell1 = completer1.cell

    cell1.trigger()
    completer1.putNext(10)

    cell1.whenNextSequential(cell1, _ => {
      NoOutcome
    })

    var fut = pool.quiescentResolveCycles
    Await.ready(fut, 2.seconds)

    Thread.sleep(200)

    fut = pool.quiescentResolveDefaults
    Await.ready(fut, 2.seconds)
  }

  test("cell dependency on itself whenNext") {
    implicit val pool = new HandlerPool(parallelism = 1)
    val completer1 = CellCompleter[ReactivePropertyStoreKey, Int](new ReactivePropertyStoreKey())
    val completer2 = CellCompleter[ReactivePropertyStoreKey, Int](new ReactivePropertyStoreKey())
    val cell1 = completer1.cell
    val cell2 = completer2.cell

    val completer10 = CellCompleter[ReactivePropertyStoreKey, Int](new ReactivePropertyStoreKey())
    val completer20 = CellCompleter[ReactivePropertyStoreKey, Int](new ReactivePropertyStoreKey())
    val cell10 = completer10.cell
    val cell20 = completer20.cell

    completer2.putNext(1)
    cell2.whenNext(cell1, x => {
      if (x == 42) {
        completer2.putFinal(43)
      }
      NoOutcome
    })

    completer20.putNext(1)
    cell20.whenNextSequential(cell10, x => {
      if (x == 10) {
        completer20.putFinal(43)
      }
      NoOutcome
    })

    completer1.putNext(10)

    cell1.whenNext(cell1, _ => {
      NoOutcome
    })

    var fut = pool.quiescentResolveCycles
    Await.ready(fut, 2.seconds)

    Thread.sleep(200)

    fut = pool.quiescentResolveDefaults
    Await.ready(fut, 10.seconds)
  }

  class ReactivePropertyStoreKey extends Key[Int] {
    override def resolve[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] = {
      cells.map((_, 42))
    }

    override def fallback[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] = {
      cells.map(cell ⇒ (cell, cell.getResult()))
    }

    override def toString = "ReactivePropertyStoreKey"
  }

  test("cell dependency on itself whenNextSequential using fallback only") {
    implicit val pool = new HandlerPool(parallelism = 8)
    val completer1 = CellCompleter[ReactivePropertyStoreKey, Int](new ReactivePropertyStoreKey())
    val cell1 = completer1.cell

    cell1.trigger()
    completer1.putNext(10)

    cell1.whenNextSequential(cell1, _ => {
      NoOutcome
    })

    val fut = pool.quiescentResolveDefaults
    Await.ready(fut, 2.seconds)
  }

  test("HandlerPool must be able to interrupt") {
    implicit val pool = new HandlerPool(parallelism = 8)
    val completer1 = CellCompleter[ReactivePropertyStoreKey, Int](new ReactivePropertyStoreKey())
    val completer2 = CellCompleter[ReactivePropertyStoreKey, Int](new ReactivePropertyStoreKey())
    val cell1 = completer1.cell
    val cell2 = completer2.cell

    cell2.whenNextSequential(cell1, v => {
      NextOutcome(v)
    })

    pool.interrupt()
    Thread.sleep(200)
    completer1.putNext(10)

    assert(cell2.getResult() == 0)

    pool.resume()

    val fut = pool.quiescentResolveDefaults
    Await.ready(fut, 2.seconds)

    assert(cell2.getResult() == 10)
  }

}
