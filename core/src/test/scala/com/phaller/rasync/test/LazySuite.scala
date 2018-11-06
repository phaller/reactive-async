package com.phaller.rasync
package test

import java.util.concurrent.CountDownLatch

import com.phaller.rasync.cell.{ Cell, FinalOutcome, NextOutcome, NoOutcome }
import com.phaller.rasync.lattice.{ DefaultKey, Updater }
import com.phaller.rasync.pool.HandlerPool
import com.phaller.rasync.test.lattice.{ IntUpdater, StringIntKey }
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration._

class LazySuite extends FunSuite {

  implicit val stringIntUpdater: Updater[Int] = new IntUpdater

  test("lazy init") {
    val latch = new CountDownLatch(1)
    val pool = new HandlerPool[Int]
    val cell = pool.mkCell(_ => {
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
    val pool = new HandlerPool[Int]

    var cell1: Cell[Int] = null
    var cell2: Cell[Int] = null

    cell1 = pool.mkCell(_ => {
      FinalOutcome(1)
    })

    cell2 = pool.mkCell(_ => {
      cell2.when(cell1)(it => {
        if (it.head._2.get.isInstanceOf[FinalOutcome[_]]) FinalOutcome(3)
        else NoOutcome
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
    val pool = new HandlerPool[Int]

    var cell1: Cell[Int] = null
    var cell2: Cell[Int] = null

    cell1 = pool.mkCell(_ => {
      assert(false)
      FinalOutcome(-11)
    })

    cell2 = pool.mkCell(_ => {
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
    val pool = new HandlerPool[Int]

    var cell1: Cell[Int] = null
    var cell2: Cell[Int] = null

    cell1 = pool.mkCell(_ => {
      cell1.when(cell2)(it => {
        if (it.head._2.get.isInstanceOf[FinalOutcome[_]]) FinalOutcome(3)
        else NoOutcome

      })
      NextOutcome(1)
    })

    cell2 = pool.mkCell(_ => {
      cell2.when(cell1)(it => {
        if (it.head._2.get.isInstanceOf[FinalOutcome[_]]) FinalOutcome(3)
        else NoOutcome
      })
      NextOutcome(2)
    })

    cell1.onNext(_ => latch1.countDown())
    cell2.onNext(_ => latch1.countDown())

    cell1.onComplete(_ => latch2.countDown())
    cell2.onComplete(_ => latch2.countDown())

    cell2.trigger()
    latch1.await()

    val fut = pool.quiescentResolveCell
    Await.ready(fut, 2.seconds)

    latch2.await()

    assert(cell1.isComplete)
    assert(cell1.getResult() == 1)

    assert(cell2.isComplete)
    assert(cell2.getResult() == 2)

    pool.shutdown()
  }

  test("cycle deps with incoming dep") {
    val latch1 = new CountDownLatch(2)
    val latch2 = new CountDownLatch(3)
    val pool = new HandlerPool[Int]

    var cell1: Cell[Int] = null
    var cell2: Cell[Int] = null
    var cell3: Cell[Int] = null

    cell1 = pool.mkCell(_ => {
      cell1.when(cell2)(_ => NextOutcome(-1))
      NextOutcome(101)
    })

    cell2 = pool.mkCell(_ => {
      cell2.when(cell1)(_ => NextOutcome(-1))
      NextOutcome(102)
    })

    cell3 = pool.mkCell(_ => {
      cell3.when(cell1)(_ => FinalOutcome(103))
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

    val fut = pool.quiescentResolveCell
    Await.ready(fut, 2.seconds)

    latch2.await()

    assert(cell3.isComplete)
    assert(cell3.getResult() === 103)

    pool.shutdown()
  }

  test("cycle deps with incoming dep, resolve cycle first") {
    val theKey = new DefaultKey[Int]()

    val latch1 = new CountDownLatch(2)
    val latch2 = new CountDownLatch(2)
    val latch3 = new CountDownLatch(1)
    val pool = new HandlerPool[Int](theKey)

    var cell1: Cell[Int] = null
    var cell2: Cell[Int] = null
    var cell3: Cell[Int] = null

    cell1 = pool.mkCell(c => {
      c.when(cell2)(_ => {
        NextOutcome(-111)
      })
      NextOutcome(11)
    })

    cell2 = pool.mkCell(c => {
      c.when(cell1)(_ => {
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

    val fut = pool.quiescentResolveCell
    Await.ready(fut, 2.seconds)

    latch2.await()

    cell3 = pool.mkCell(c => {
      c.when(cell1)(_ => {
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
    val pool = new HandlerPool[Int]
    var c1: Cell[Int] = null
    var c2: Cell[Int] = null
    c1 = pool.mkCell(_ => {
      c1.when(c2)(_ => FinalOutcome(-2))
      FinalOutcome(-1)
    })
    c2 = pool.mkCell(_ => {
      c2.when(c1)(_ => FinalOutcome(-2))
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
  //
  test("cell does not get resolved, if not triggered") {
    val pool = new HandlerPool[Int]
    val c = pool.mkCell(_ => FinalOutcome(-1))

    val fut2 = pool.quiescentResolveCell
    Await.ready(fut2, 2.seconds)

    assert(c.getResult() == 0)
    assert(!c.isComplete)

    pool.shutdown()
  }
  //
  test("cell gets resolved, if triggered") {
    val pool = new HandlerPool[Int](new StringIntKey(""))
    val cell = pool.mkCell(_ => {
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

