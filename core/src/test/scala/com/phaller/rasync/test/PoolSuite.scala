package com.phaller.rasync
package test

import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch }

import com.phaller.rasync.cell.{ Cell, CellCompleter }
import org.scalatest.FunSuite

import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._
import com.phaller.rasync.lattice.Updater
import com.phaller.rasync.pool.HandlerPool
import com.phaller.rasync.test.lattice.{ IntUpdater, StringIntKey }

class PoolSuite extends FunSuite {
  test("onQuiescent") {
    val pool = new HandlerPool[Nothing]

    var i = 0
    while (i < 10000) {
      val p1 = Promise[Boolean]()
      val p2 = Promise[Boolean]()
      pool.execute { () => { p1.success(true) }: Unit }
      pool.onQuiescent { () => p2.success(true) }
      try {
        Await.result(p2.future, 1.seconds)
      } catch {
        case t: Throwable =>
          assert(false, s"failure after $i iterations")
      }
      i += 1
    }

    pool.shutdown()
  }

  test("register cells concurrently") {
    implicit val stringIntUpdater: Updater[Int] = new IntUpdater

    implicit val pool = new HandlerPool[Int](new StringIntKey("s"))
    var regCells = new ConcurrentHashMap[Cell[Int], Cell[Int]]()
    for (_ <- 1 to 1000) {
      pool.execute(() => {
        val completer = CellCompleter[Int]()
        completer.cell.trigger()
        regCells.put(completer.cell, completer.cell)
        ()
      })
    }
    val fut = pool.quiescentResolveCell // set all (registered) cells to 1 via key.fallback
    Await.ready(fut, 5.seconds)

    regCells.values().removeIf(_.getResult() != 0)
    assert(regCells.size === 0)
  }

  test("register cells concurrently 2") {
    implicit val stringIntUpdater: Updater[Int] = new IntUpdater

    implicit val pool = new HandlerPool[Int](new StringIntKey("s"))
    var regCells = new ConcurrentHashMap[Cell[Int], Cell[Int]]()
    for (_ <- 1 to 1000) {
      pool.execute(() => {
        val completer = CellCompleter[Int]()
        regCells.put(completer.cell, completer.cell)
        ()
      })
    }
    val fut = pool.quiescentResolveCell // set all (registered) cells to 1 via key.fallback
    Await.ready(fut, 5.seconds)

    assert(regCells.size === 1000)
  }

  test("handler pool quiescence") {
    implicit val pool = new HandlerPool
    val latch = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)
    pool.execute { () => latch.await() }
    pool.onQuiescent { () => latch2.countDown() }
    latch.countDown()

    latch2.await()
    assert(true)

    pool.onQuiescenceShutdown()
  }

}
