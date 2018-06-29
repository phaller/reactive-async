package com.phaller.rasync
package test

import java.util.concurrent.CountDownLatch

import com.phaller.rasync.lattice.{ DefaultKey, Updater }
import com.phaller.rasync.test.lattice.{ StringIntKey, StringIntUpdater }
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration._

class LazySuite extends FunSuite {

  implicit val stringIntUpdater: Updater[Int] = new StringIntUpdater

  test("lazy init") {
    val latch = new CountDownLatch(1)
    val pool = new HandlerPool()
    val cell = pool.mkCell[StringIntKey, Int]("cell", _ => {
      FinalOutcome(1)
    })
    cell.onComplete(_ => latch.countDown())

    assert(!cell.isComplete)
    cell.trigger()

    latch.await()

    assert(cell.isComplete)
    assert(cell.getResult() == 1)

    pool.shutdown()
  }

  test("trigger dependees") {
    val latch = new CountDownLatch(2)
    val pool = new HandlerPool()

    var cell1: Cell[StringIntKey, Int] = null
    var cell2: Cell[StringIntKey, Int] = null

    cell1 = pool.mkCell[StringIntKey, Int]("cell1", _ => {
      FinalOutcome(1)
    })

    cell2 = pool.mkCell[StringIntKey, Int]("cell2", _ => {
      cell2.whenComplete(cell1, _ => {
        FinalOutcome(3)
      })
      NextOutcome(2)
    })

    cell1.onComplete(_ => latch.countDown())
    cell2.onComplete(_ => latch.countDown())

    assert(!cell1.isComplete)
    assert(!cell2.isComplete)
    cell2.trigger()

    latch.await()

    assert(cell1.isComplete)
    assert(cell1.getResult() == 1)

    assert(cell2.isComplete)
    assert(cell2.getResult() == 3)

    pool.shutdown()
  }

  test("do not trigger unneeded cells") {
    val latch = new CountDownLatch(1)
    val pool = new HandlerPool()

    var cell1: Cell[StringIntKey, Int] = null
    var cell2: Cell[StringIntKey, Int] = null

    cell1 = pool.mkCell[StringIntKey, Int]("cell1", _ => {
      assert(false)
      FinalOutcome(-11)
    })

    cell2 = pool.mkCell[StringIntKey, Int]("cell2", _ => {
      FinalOutcome(2)
    })

    cell2.onComplete(_ => latch.countDown())

    cell2.trigger()

    latch.await()

    assert(!cell1.isComplete)
    assert(cell1.getResult() == 0)

    pool.shutdown()
  }

  test("cycle deps") {
    val latch1 = new CountDownLatch(2)
    val latch2 = new CountDownLatch(2)
    val pool = new HandlerPool()

    var cell1: Cell[StringIntKey, Int] = null
    var cell2: Cell[StringIntKey, Int] = null

    cell1 = pool.mkCell[StringIntKey, Int]("cell1", _ => {
      cell1.whenComplete(cell2, _ => {
        FinalOutcome(3)
      })
      NextOutcome(1)
    })

    cell2 = pool.mkCell[StringIntKey, Int]("cell2", _ => {
      cell2.whenComplete(cell1, _ => {
        FinalOutcome(3)
      })
      NextOutcome(2)
    })

    cell1.onNext(_ => latch1.countDown())
    cell2.onNext(_ => latch1.countDown())

    cell1.onComplete(_ => latch2.countDown())
    cell2.onComplete(_ => latch2.countDown())

    cell2.trigger()
    latch1.await()

    val fut = pool.quiescentResolveCycles
    val ready = Await.ready(fut, 2.seconds)

    latch2.await()

    assert(cell1.isComplete)
    assert(cell1.getResult() == 0)

    assert(cell2.isComplete)
    assert(cell2.getResult() == 0)

    pool.shutdown()
  }

  test("cycle deps with outgoing dep") {
    val theKey = new DefaultKey[Int]()

    val latch1 = new CountDownLatch(2)
    val latch2 = new CountDownLatch(3)
    val pool = new HandlerPool()

    var cell1: Cell[theKey.type, Int] = null
    var cell2: Cell[theKey.type, Int] = null
    var cell3: Cell[theKey.type, Int] = null

    cell1 = pool.mkCell[theKey.type, Int](theKey, _ => {
      cell1.whenComplete(cell2, _ => NextOutcome(-1))
      NextOutcome(101)
    })

    cell2 = pool.mkCell[theKey.type, Int](theKey, _ => {
      cell2.whenComplete(cell1, _ => NextOutcome(-1))
      NextOutcome(102)
    })

    cell3 = pool.mkCell[theKey.type, Int](theKey, _ => {
      cell3.whenComplete(cell1, _ => FinalOutcome(103))
      NextOutcome(-1)
    })

    cell1.onNext(_ => latch1.countDown())
    cell2.onNext(_ => latch1.countDown())

    cell1.onComplete(_ => latch2.countDown())
    cell2.onComplete(_ => latch2.countDown())
    cell3.onComplete(_ => latch2.countDown())

    assert(!cell1.isComplete)
    assert(!cell2.isComplete)

    cell3.trigger()
    latch1.await()

    val fut = pool.quiescentResolveCycles
    val ready = Await.ready(fut, 2.seconds)

    latch2.await()

    assert(cell3.isComplete)
    assert(cell3.getResult() === 103)

    pool.shutdown()
  }

  test("cycle deps with outgoing dep, resolve cycle first") {
    val theKey = new DefaultKey[Int]()

    val latch1 = new CountDownLatch(2)
    val latch2 = new CountDownLatch(2)
    val latch3 = new CountDownLatch(1)
    val pool = new HandlerPool()

    var cell1: Cell[theKey.type, Int] = null
    var cell2: Cell[theKey.type, Int] = null
    var cell3: Cell[theKey.type, Int] = null

    cell1 = pool.mkCell[theKey.type, Int](theKey, c => {
      c.whenComplete(cell2, _ => {
        NextOutcome(-111)
      })
      NextOutcome(11)
    })

    cell2 = pool.mkCell[theKey.type, Int](theKey, c => {
      c.whenComplete(cell1, _ => {
        NextOutcome(-222)
      })
      NextOutcome(22)
    })

    cell1.onNext(_ => latch1.countDown())
    cell2.onNext(_ => latch1.countDown())

    cell1.onComplete(_ => latch2.countDown())
    cell2.onComplete(_ => latch2.countDown())

    cell2.trigger()
    latch1.await()

    val fut = pool.quiescentResolveCycles
    val ready = Await.ready(fut, 2.seconds)

    latch2.await()

    cell3 = pool.mkCell[theKey.type, Int](theKey, c => {
      c.whenComplete(cell1, _ => {
        FinalOutcome(333)
      })
      NextOutcome(-3)
    })

    cell3.onComplete(_ => latch3.countDown())
    cell3.trigger()

    latch3.await()

    assert(cell3.isComplete)
    assert(cell3.getResult() === 333)

    pool.shutdown()
  }

  test("cycle does not get resolved, if not triggered") {
    val pool = new HandlerPool()
    var c1: Cell[StringIntKey, Int] = null
    var c2: Cell[StringIntKey, Int] = null
    c1 = pool.mkCell[StringIntKey, Int]("cell1", _ => {
      c1.whenNext(c2, _ => FinalOutcome(-2))
      FinalOutcome(-1)
    })
    c2 = pool.mkCell[StringIntKey, Int]("cell2", _ => {
      c2.whenNext(c1, _ => FinalOutcome(-2))
      FinalOutcome(-1)
    })

    val fut2 = pool.quiescentResolveCell
    Await.ready(fut2, 2.seconds)

    assert(c1.getResult() == 0)
    assert(!c1.isComplete)
    assert(c2.getResult() == 0)
    assert(!c2.isComplete)

    pool.shutdown()
  }

  test("cell does not get resolved, if not triggered") {
    val pool = new HandlerPool()
    val c = pool.mkCell[StringIntKey, Int]("cell1", _ => FinalOutcome(-1))

    val fut2 = pool.quiescentResolveCell
    Await.ready(fut2, 2.seconds)

    assert(c.getResult() == 0)
    assert(!c.isComplete)

    pool.shutdown()
  }

  test("cell gets resolved, if triggered") {
    val pool = new HandlerPool()
    val cell = pool.mkCell[StringIntKey, Int]("cell1", _ => {
      NextOutcome(-1)
    })
    cell.trigger()

    val fut2 = pool.quiescentResolveCell
    Await.ready(fut2, 2.seconds)

    assert(cell.isComplete) // cell should be completed with a fallback value
    assert(cell.getResult() == 1) // StringIntKey sets cell to fallback value `1`.

    pool.shutdown()
  }
}
