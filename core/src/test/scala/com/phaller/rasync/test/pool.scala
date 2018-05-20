package com.phaller.rasync
package test

import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch }

import org.scalatest.FunSuite

import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._

import scala.util.{ Failure, Success }
import lattice.{ StringIntKey, StringIntUpdater, Updater }

class PoolSuite extends FunSuite {
  test("onQuiescent") {
    val pool = new HandlerPool

    var i = 0
    while (i < 10000) {
      val p1 = Promise[Boolean]()
      val p2 = Promise[Boolean]()
      pool.execute { () => { p1.success(true); () } }
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
    implicit val stringIntUpdater: Updater[Int] = new StringIntUpdater

    implicit val pool = new HandlerPool()
    var regCells = new ConcurrentHashMap[Cell[StringIntKey, Int], Cell[StringIntKey, Int]]()
    for (_ <- 1 to 1000) {
      pool.execute(() => {
        val completer = CellCompleter[StringIntKey, Int]("somekey")
        completer.cell.trigger()
        regCells.put(completer.cell, completer.cell)
        ()
      })
    }
    val fut = pool.quiescentResolveDefaults // set all (registered) cells to 1 via key.fallback
    Await.ready(fut, 5.seconds)

    regCells.values().removeIf(_.getResult() != 0)
    assert(regCells.size === 0)
  }

  test("register cells concurrently 2") {
    implicit val stringIntUpdater: Updater[Int] = new StringIntUpdater

    implicit val pool = new HandlerPool()
    var regCells = new ConcurrentHashMap[Cell[StringIntKey, Int], Cell[StringIntKey, Int]]()
    for (_ <- 1 to 1000) {
      pool.execute(() => {
        val completer = CellCompleter[StringIntKey, Int]("somekey")
        regCells.put(completer.cell, completer.cell)
        ()
      })
    }
    val fut = pool.quiescentResolveDefaults // set all (registered) cells to 1 via key.fallback
    Await.ready(fut, 5.seconds)

    assert(regCells.size === 1000)
  }

  test("onQuiescent(cell): incomplete cell") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")(new StringIntUpdater, pool)

    var i = 0
    while (i < 10000) {
      val p1 = Promise[Boolean]()
      val p2 = Promise[Boolean]()
      pool.execute { () => { p1.success(true); () } }
      pool.onQuiescent { () => p2.success(true) }
      try {
        Await.result(p2.future, 1.seconds)
      } catch {
        case t: Throwable =>
          assert(false, s"failure after $i iterations")
      }
      i += 1
    }

    pool.onQuiescent(completer1.cell) {
      case Success(x) =>
        assert(x === 0)
        latch.countDown()
      case Failure(_) => assert(false)
    }

    latch.await()

    pool.shutdown()
  }

  test("onQuiescent(cell): completed cell") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter.completed[Int](10)(new StringIntUpdater, pool)

    var i = 0
    while (i < 10000) {
      val p1 = Promise[Boolean]()
      val p2 = Promise[Boolean]()
      pool.execute { () => { p1.success(true); () } }
      pool.onQuiescent { () => p2.success(true) }
      try {
        Await.result(p2.future, 1.seconds)
      } catch {
        case t: Throwable =>
          assert(false, s"failure after $i iterations")
      }
      i += 1
    }

    pool.onQuiescent(completer1.cell) {
      case Success(x) =>
        assert(x === 10)
        latch.countDown()
      case Failure(_) => assert(false)
    }

    latch.await()

    pool.shutdown()
  }

  test("onQuiescent(cell): incomplete cell, added in non-quiescent state") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")(new StringIntUpdater, pool)

    pool.execute(() => {
      // Add all tasks and handler in this thread to ensure
      // that the pool is not quiescent while adding.

      var i = 0
      while (i < 10000) {
        val p1 = Promise[Boolean]()
        val p2 = Promise[Boolean]()
        pool.execute { () => { p1.success(true); () } }
        i += 1
      }

      pool.onQuiescent(completer1.cell) {
        case Success(x) =>
          assert(x === 0)
          latch.countDown()
        case Failure(_) => assert(false)
      }
    })

    latch.await()

    pool.shutdown()
  }

  test("onQuiescent(cell): completed cell, added in non-quiescent state") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter.completed[Int](10)(new StringIntUpdater, pool)

    pool.execute(() => {
      // Add all tasks and handler in this thread to ensure
      // that the pool is not quiescent while adding.

      var i = 0
      while (i < 10000) {
        val p1 = Promise[Boolean]()
        val p2 = Promise[Boolean]()
        pool.execute { () => { p1.success(true); () } }
        i += 1
      }

      pool.onQuiescent(completer1.cell) {
        case Success(x) =>
          assert(x === 10)
          latch.countDown()
        case Failure(_) => assert(false)
      }
    })

    latch.await()

    pool.shutdown()
  }

}
