package com.phaller.rasync
package test

import org.scalatest.FunSuite
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.AtomicInteger

import scala.util.{ Failure, Success }
import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._
import lattice._

class BaseSuite extends FunSuite {

  implicit val stringIntUpdater: Updater[Int] = new StringIntUpdater

  test("putFinal") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int]("somekey")
    val cell = completer.cell
    cell.onComplete {
      case Success(v) =>
        assert(v === 5)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    completer.putFinal(5)

    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("putFinal: 2 putFinals with same value to the same cell") {
    implicit val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int]("somekey")
    val cell = completer.cell

    completer.putFinal(5)

    try {
      completer.putFinal(5)
      assert(true)
    } catch {
      case e: Exception => assert(false)
    }

    pool.onQuiescenceShutdown()
  }

  test("putFinal: putFinal on complete cell without adding new information") {
    implicit val pool = new HandlerPool
    val completer = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
    val cell = completer.cell

    completer.putFinal(Mutable)

    try {
      completer.putFinal(ConditionallyImmutable)
      assert(true)
    } catch {
      case ise: IllegalStateException => assert(false)
      case e: Exception => assert(false)
    }

    pool.onQuiescenceShutdown()
  }

  //  test("putFinal: putFinal on complete cell adding new information") {
  //  We do not throw any more
  //    implicit val pool = new HandlerPool
  //    val completer = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
  //    val cell = completer.cell
  //
  //    completer.putFinal(ConditionallyImmutable)
  //
  //    try {
  //      completer.putFinal(Mutable)
  //      assert(false)
  //    } catch {
  //      case ise: IllegalStateException => assert(true)
  //      case e: Exception => assert(false)
  //    }
  //
  //    pool.onQuiescenceShutdown()
  //  }

  test("whenComplete") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenComplete(completer2.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)

    cell1.onComplete {
      case Success(v) =>
        assert(v === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putFinal(10)

    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenCompleteSequential") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenCompleteSequential(completer2.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)

    cell1.onComplete {
      case Success(v) =>
        assert(v === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putFinal(10)

    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenComplete: dependency 1") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenComplete(completer2.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)

    cell1.onComplete {
      case Success(v) =>
        assert(v === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putFinal(10)

    latch.await()

    assert(cell1.numCompleteDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("whenCompleteSequential: dependency 1") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenCompleteSequential(completer2.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)

    cell1.onComplete {
      case Success(v) =>
        assert(v === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putFinal(10)

    latch.await()

    assert(cell1.numCompleteDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("whenComplete: dependency 2") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenComplete(completer2.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)

    completer2.putFinal(9)

    cell1.waitUntilNoDeps()

    assert(cell1.numCompleteDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("whenCompleteSequential: dependency 2") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenCompleteSequential(completer2.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)

    completer2.putFinal(9)

    cell1.waitUntilNoDeps()

    assert(cell1.numCompleteDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("whenComplete: dependency 3") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenComplete(completer2.cell, x => if (x == 10) NextOutcome(20) else NoOutcome)

    completer2.putFinal(10)

    cell1.waitUntilNoDeps()

    assert(cell1.numCompleteDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("whenCompleteSequential: dependency 3") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenCompleteSequential(completer2.cell, x => if (x == 10) NextOutcome(20) else NoOutcome)

    completer2.putFinal(10)

    cell1.waitUntilNoDeps()

    assert(cell1.numCompleteDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("whenNext/whenComplete: dependency ") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")
    val completer3 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenComplete(completer2.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)
    cell1.whenNext(completer2.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)
    cell1.whenCompleteSequential(completer2.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)
    cell1.whenNextSequential(completer2.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)

    cell1.whenNext(completer3.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)
    cell1.whenComplete(completer3.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)
    cell1.whenNextSequential(completer3.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)
    cell1.whenCompleteSequential(completer3.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)

    assert(cell1.numCompleteDependencies == 2)
    assert(cell1.numNextDependencies == 2)

    pool.shutdown()
  }

  test("whenComplete: callback removal") {
    implicit val pool = new HandlerPool

    val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
    val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

    completer1.cell.whenComplete(completer2.cell, (imm) => if (imm == Mutable) FinalOutcome(Mutable) else NoOutcome)

    completer1.putFinal(Immutable)
    assert(completer2.cell.numCompleteDependentCells == 0)
    completer2.putFinal(Mutable)

    assert(completer1.cell.getResult() == Immutable)
    assert(completer2.cell.getResult() == Mutable)

    pool.onQuiescenceShutdown()
  }

  test("whenCompleteSequential: callback removal") {
    implicit val pool = new HandlerPool

    val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
    val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

    completer1.cell.whenCompleteSequential(completer2.cell, (imm) => if (imm == Mutable) FinalOutcome(Mutable) else NoOutcome)

    completer1.putFinal(Immutable)
    assert(completer2.cell.numCompleteDependentCells == 0)
    completer2.putFinal(Mutable)

    assert(completer1.cell.getResult() == Immutable)
    assert(completer2.cell.getResult() == Mutable)

    pool.onQuiescenceShutdown()
  }

  test("onNext") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int]("somekey")

    val cell = completer.cell

    cell.onNext {
      case Success(x) =>
        assert(x === 9)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer.putNext(9)

    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNext") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, x => {
      if (x == 10) NextOutcome(20)
      else NoOutcome
    })

    cell1.onNext {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putNext(10)
    latch.await()

    assert(cell1.numNextDependencies == 1)

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNextSequential(completer2.cell, x => {
      if (x == 10) NextOutcome(20)
      else NoOutcome
    })

    cell1.onNext {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putNext(10)
    latch.await()

    assert(cell1.numNextDependencies == 1)

    pool.onQuiescenceShutdown()
  }

  test("whenNext: dependency 1") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) NextOutcome(20)
      else NoOutcome
    })

    cell1.onNext {
      case Success(x) =>
        assert(x === 8)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putNext(9)
    completer1.putNext(8)

    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: dependency 1") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNextSequential(completer2.cell, (x: Int) => {
      if (x == 10) NextOutcome(20)
      else NoOutcome
    })

    cell1.onNext {
      case Success(x) =>
        assert(x === 8)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putNext(9)
    completer1.putNext(8)

    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNext: dependency 2") {
    // TODO ! blinker?
    //    for (i <- 1 to 100)
    {
      //      println(s"starting round $i")
      val latch = new CountDownLatch(2)

      implicit val pool = new HandlerPool
      val completer1 = CellCompleter[StringIntKey, Int]("somekey")
      val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

      val cell1 = completer1.cell
      cell1.whenNext(completer2.cell, x => {
        if (x == 10) NextOutcome(20)
        else NoOutcome
      })
      cell1.whenComplete(completer2.cell, x => {
        if (x == 10) FinalOutcome(20)
        else NoOutcome
      })

      assert(cell1.numNextDependencies == 1)

      cell1.onComplete {
        case Success(x) =>
          println("on complete called")
          assert(x === 20)
          latch.countDown()
        case Failure(e) =>
          assert(false)
          latch.countDown()
      }
      cell1.onNext {
        case Success(x) =>
          println("on next called")
          assert(x === 20)
          latch.countDown()
        case Failure(e) =>
          assert(false)
          latch.countDown()
      }

      completer2.putFinal(10)
      latch.await()

      assert(cell1.numNextDependencies == 0)

      pool.onQuiescenceShutdown()
    }
  }

  test("whenNextSequential: dependency 2") {
    val latch = new CountDownLatch(2)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNextSequential(completer2.cell, (x: Int) => {
      if (x == 10) NextOutcome(20)
      else NoOutcome
    })
    cell1.whenCompleteSequential(completer2.cell, x => if (x == 10) FinalOutcome(20) else NoOutcome)

    assert(cell1.numNextDependencies == 1)

    cell1.onComplete {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    cell1.onNext {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putFinal(10)
    latch.await()

    assert(cell1.numNextDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("whenNext: Triggered by putFinal when no whenComplete exist for same cell") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) NextOutcome(20)
      else NoOutcome
    })

    assert(cell1.numNextDependencies == 1)

    cell1.onNext {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putFinal(10)
    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: Triggered by putFinal when no whenComplete exist for same cell") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNextSequential(completer2.cell, (x: Int) => {
      if (x == 10) NextOutcome(20)
      else NoOutcome
    })

    assert(cell1.numNextDependencies == 1)

    cell1.onNext {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putFinal(10)
    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNext: Remove whenNext dependencies from cells that depend on a completing cell") {
    /* Needs the dependees from the feature/cscc-resolving branch */
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, x => {
      if (x == 10) NextOutcome(20)
      else NoOutcome
    })

    completer2.putFinal(10)

    cell1.waitUntilNoNextDeps()

    assert(cell1.numNextDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: Remove whenNext dependencies from cells that depend on a completing cell") {
    /* Needs the dependees from the feature/cscc-resolving branch */
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNextSequential(completer2.cell, x => {
      if (x == 10) NextOutcome(20)
      else NoOutcome
    })

    completer2.putFinal(10)

    cell1.waitUntilNoNextDeps()

    assert(cell1.numNextDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("whenNext: callback removal") {
    implicit val pool = new HandlerPool

    val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
    val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

    completer1.cell.whenNext(completer2.cell, (imm: Immutability) => imm match {
      case Mutable => NextOutcome(Mutable)
      case _ => NoOutcome
    })

    completer1.putFinal(Immutable)
    assert(completer2.cell.numNextDependentCells == 0)
    completer2.putNext(Mutable)

    assert(completer1.cell.getResult() == Immutable)
    assert(completer2.cell.getResult() == Mutable)

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: callback removal") {
    implicit val pool = new HandlerPool

    val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
    val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

    completer1.cell.whenNextSequential(completer2.cell, (imm: Immutability) => imm match {
      case Mutable => NextOutcome(Mutable)
      case _ => NoOutcome
    })

    completer1.putFinal(Immutable)
    assert(completer2.cell.numNextDependencies == 0)
    completer2.putNext(Mutable)

    assert(completer1.cell.getResult() == Immutable)
    assert(completer2.cell.getResult() == Mutable)

    pool.onQuiescenceShutdown()
  }

  test("whenNext: Dependencies concurrency test") {
    implicit val pool = new HandlerPool
    val latch = new CountDownLatch(10000)
    val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

    for (i <- 1 to 10000) {
      pool.execute(() => {
        val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
        completer1.cell.whenNext(completer2.cell, (x: Immutability) => {
          if (x == Mutable) NextOutcome(Mutable)
          else NoOutcome
        })
        latch.countDown()
      })
    }

    latch.await()

    assert(completer1.cell.numNextDependencies == 10000)

    pool.onQuiescenceShutdown()
  }

  test("whenComplete: Dependencies concurrency test") {
    implicit val pool = new HandlerPool
    val latch = new CountDownLatch(10000)
    val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

    for (i <- 1 to 10000) {
      pool.execute(() => {
        val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
        completer1.cell.whenComplete(completer2.cell, (x: Immutability) => {
          if (x == Mutable) NextOutcome(Mutable)
          else NoOutcome
        })
        latch.countDown()
      })
    }

    latch.await()

    assert(completer1.cell.numCompleteDependencies == 10000)

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: Dependencies concurrency test") {
    implicit val pool = new HandlerPool
    val latch = new CountDownLatch(10000)
    val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
    val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

    for (i <- 1 to 10000) {
      pool.execute(() => {
        val otherCell = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey).cell
        completer1.cell.whenNextSequential(otherCell, (x: Immutability) => {
          if (x == Mutable) NextOutcome(Mutable)
          else NoOutcome
        })
        latch.countDown()
      })
    }

    latch.await()

    assert(completer1.cell.numNextDependencies == 10000)

    pool.onQuiescenceShutdown()
  }

  test("whenNext: concurrent put final") {
    var expectedValue: Option[Immutability] = None

    for (_ <- 1 to 100) {
      implicit val pool = new HandlerPool
      val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
      val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

      val cell1 = completer1.cell
      cell1.trigger()

      pool.execute(() => cell1.whenComplete(completer2.cell, x => {
        NoOutcome
      }))

      pool.execute(() => cell1.whenNext(completer2.cell, x => {
        if (x == Mutable) FinalOutcome(Mutable)
        else NoOutcome
      }))
      pool.execute(() => completer2.putFinal(Mutable))

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 2.seconds)

      if (expectedValue.isEmpty) expectedValue = Some(cell1.getResult())
      else assert(cell1.getResult() == expectedValue.get)

      pool.onQuiescenceShutdown()
    }
  }

  test("whenNextSequential: concurrent put final") {
    var expectedValue: Option[Immutability] = None

    for (_ <- 1 to 100) {
      implicit val pool = new HandlerPool
      val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
      val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

      val cell1 = completer1.cell

      pool.execute(() => cell1.whenCompleteSequential(completer2.cell, x => {
        NoOutcome
      }))

      pool.execute(() => cell1.whenNextSequential(completer2.cell, x => {
        if (x == Mutable) FinalOutcome(Mutable)
        else NoOutcome
      }))
      pool.execute(() => completer2.putFinal(Mutable))

      val fut = pool.quiescentResolveCycles
      Await.ready(fut, 2.seconds)

      if (expectedValue.isEmpty) expectedValue = Some(cell1.getResult())
      else assert(cell1.getResult() == expectedValue.get)

      pool.onQuiescenceShutdown()
    }
  }

  test("whenNext: One cell with several dependencies on the same cell concurrency test") {
    implicit val pool = new HandlerPool

    for (i <- 1 to 1000) {
      val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
      val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
      completer1.cell.whenNext(completer2.cell, {
        case Immutable | ConditionallyImmutable => NoOutcome
        case Mutable => NextOutcome(Mutable)
      })

      assert(completer1.cell.numTotalDependencies == 1)

      pool.execute(() => completer2.putNext(ConditionallyImmutable))
      pool.execute(() => completer2.putFinal(Mutable))

      val fut = pool.quiescentResolveCell
      Await.result(fut, 2.second)

      assert(completer2.cell.getResult() == Mutable)
      assert(completer1.cell.getResult() == Mutable)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNext: concurrent put next") {
    var expectedValue: Option[Immutability] = None

    for (_ <- 1 to 100) {
      implicit val pool = new HandlerPool
      val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
      val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

      val cell1 = completer1.cell

      pool.execute(() => cell1.whenNext(completer2.cell, x => {
        if (x == Mutable) NextOutcome(Mutable)
        else NoOutcome
      }))
      pool.execute(() => completer2.putNext(Mutable))

      val fut = pool.quiescentResolveDefaults
      Await.ready(fut, 2.seconds)

      if (expectedValue.isEmpty) expectedValue = Some(cell1.getResult())
      else assert(cell1.getResult() == expectedValue.get)

      pool.shutdown()
    }
  }

  test("whenNextSequential: One cell with several dependencies on the same cell concurrency test") {
    implicit val pool = new HandlerPool

    var numErrorsWithCell1 = 0
    var numErrorsWithCell2 = 0

    for (i <- 1 to 1000) {
      val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
      val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
      completer1.cell.whenNextSequential(completer2.cell, {
        case Immutable | ConditionallyImmutable => NoOutcome
        case Mutable => NextOutcome(Mutable)
      })

      assert(completer1.cell.numTotalDependencies == 1)

      pool.execute(() => completer2.putNext(ConditionallyImmutable))
      pool.execute(() => completer2.putFinal(Mutable))

      val fut = pool.quiescentResolveCell
      Await.result(fut, 2.second)

      if (!(completer1.cell.getResult() == Mutable)) numErrorsWithCell1 += 1
      if (!(completer2.cell.getResult() == Mutable)) numErrorsWithCell2 += 1
    }
    assert(numErrorsWithCell1 == 0, "errors with cell 1")
    assert(numErrorsWithCell2 == 0, "errors with cell 2")
    pool.onQuiescenceShutdown()
  }

  //  Those tests is not correct any more.
  //  test("whenNext: complete dependent cell - forced update") {
  //
  //    val latch = new CountDownLatch(1)
  //
  //    implicit val pool = new HandlerPool
  //    val completer1 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)(Updater.latticeToUpdater(new NaturalNumberLattice), pool)
  //    val completer2 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)(Updater.latticeToUpdater(new NaturalNumberLattice), pool)
  //
  //    val cell1 = completer1.cell
  //    cell1.whenNext(completer2.cell, (x: Int) => {
  //      if (x == 10) FinalOutcome(20)
  //      else NoOutcome
  //    })
  //
  //    cell1.onComplete {
  //      case Success(v) =>
  //        assert(v === 20)
  //        latch.countDown()
  //      case Failure(e) =>
  //        assert(false)
  //        latch.countDown()
  //    }
  //
  //    // This will complete `cell1`
  //    completer2.putNext(10)
  //
  //    latch.await()
  //
  //    // cell1 should be completed, so putNext(5) should not succeed
  //    try {
  //      completer1.putNext(50)
  //      assert(false)
  //    } catch {
  //      case ise: IllegalStateException => assert(true)
  //      case e: Exception => assert(false)
  //    }
  //
  //    pool.onQuiescenceShutdown()
  //  }
  //
  //  test("whenNextSequential: complete dependent cell - forced update") {
  //    val latch = new CountDownLatch(1)
  //
  //    implicit val pool = new HandlerPool
  //    val completer1 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)(Updater.latticeToUpdater(new NaturalNumberLattice), pool)
  //    val completer2 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)(Updater.latticeToUpdater(new NaturalNumberLattice), pool)
  //
  //    val cell1 = completer1.cell
  //    cell1.whenNextSequential(completer2.cell, (x: Int) => {
  //      if (x == 10) FinalOutcome(20)
  //      else NoOutcome
  //    })
  //
  //    cell1.onComplete {
  //      case Success(v) =>
  //        assert(v === 20)
  //        latch.countDown()
  //      case Failure(e) =>
  //        assert(false)
  //        latch.countDown()
  //    }
  //
  //    // This will complete `cell1`
  //    completer2.putNext(10)
  //
  //    latch.await()
  //
  //    // cell1 should be completed, so putNext(5) should not succeed
  //    try {
  //      completer1.putNext(50)
  //      assert(false)
  //    } catch {
  //      case ise: IllegalStateException => assert(true)
  //      case e: Exception => assert(false)
  //    }
  //
  //    pool.onQuiescenceShutdown()
  //  }

  test("whenNext: complete dependent cell - ignored update") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) FinalOutcome(20)
      else NoOutcome
    })

    cell1.onComplete {
      case Success(v) =>
        assert(v === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    // This will complete `cell1`
    completer2.putNext(10)

    latch.await()

    // the invocation of putNext is ignored, because cell1 is already completed
    // note that the implicitly used StringIntUpdater has ignoreIfFinal==true
    try {
      completer1.putNext(50)
    } catch {
      case ise: IllegalStateException => assert(false)
      case e: Exception => assert(false)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: complete dependent cell - ignored update") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNextSequential(completer2.cell, (x: Int) => {
      if (x == 10) FinalOutcome(20)
      else NoOutcome
    })

    cell1.onComplete {
      case Success(v) =>
        assert(v === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    // This will complete `cell1`
    completer2.putNext(10)

    latch.await()

    // the invocation of putNext is ignored, because cell1 is already completed
    // note that the implicitly used StringIntUpdater has ignoreIfFinal==true
    try {
      completer1.putNext(50)
    } catch {
      case ise: IllegalStateException => assert(false)
      case e: Exception => assert(false)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNext: complete depedent cell, dependency 1") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) FinalOutcome(20)
      else NoOutcome
    })

    cell1.onComplete {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putNext(9)
    completer2.putNext(10)

    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: complete depedent cell, dependency 1") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNextSequential(completer2.cell, (x: Int) => {
      if (x == 10) FinalOutcome(20)
      else NoOutcome
    })

    cell1.onComplete {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putNext(9)
    completer2.putNext(10)

    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNext: complete dependent cell, dependency 2") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) FinalOutcome(20)
      else NoOutcome
    })

    cell1.onNext {
      case Success(x) =>
        assert(x === 8 || x === 20)
        latch1.countDown()
      case Failure(e) =>
        assert(false)
        latch1.countDown()
    }

    cell1.onComplete {
      case Success(x) =>
        assert(x === 20)
        latch2.countDown()
      case Failure(e) =>
        assert(false)
        latch2.countDown()
    }

    completer1.putNext(8)
    latch1.await()

    completer2.putNext(10)
    latch2.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: complete dependent cell, dependency 2") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenNextSequential(completer2.cell, (x: Int) => {
      if (x == 10) FinalOutcome(20)
      else NoOutcome
    })

    cell1.onNext {
      case Success(x) =>
        assert(x === 8 || x === 20)
        latch1.countDown()
      case Failure(e) =>
        assert(false)
        latch1.countDown()
    }

    cell1.onComplete {
      case Success(x) =>
        assert(x === 20)
        latch2.countDown()
      case Failure(e) =>
        assert(false)
        latch2.countDown()
    }

    completer1.putNext(8)
    latch1.await()

    completer2.putNext(10)
    latch2.await()

    pool.onQuiescenceShutdown()
  }

  test("when: values passed to callback") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.when(completer2.cell, (x, isFinal) => {
      Outcome(x, isFinal) // complete, if completer2 is completed
    })

    assert(cell1.numNextDependencies == 1)
    assert(cell1.numTotalDependencies == 1)
    //    assert(completer2.cell.numNextCallbacks == 1)
    //    assert(completer2.cell.numCompleteCallbacks == 1)

    cell1.onNext {
      case Success(x) =>
        assert((x === 8 && !cell1.isComplete) || x === 10)
        latch1.countDown()
      case Failure(e) =>
        assert(false)
        latch1.countDown()
    }

    cell1.onComplete {
      case Success(x) =>
        assert(x === 10)
        latch2.countDown()
      case Failure(e) =>
        assert(false)
        latch2.countDown()
    }

    completer1.putNext(8)
    latch1.await()

    assert(!cell1.isComplete)

    completer2.putFinal(10)
    latch2.await()

    assert(cell1.isComplete)

    pool.onQuiescenceShutdown()
  }

  test("whenSequential: values passed to callback") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenSequential(completer2.cell, (x, isFinal) => {
      Outcome(x, isFinal) // complete, if completer2 is completed
    })

    assert(cell1.numNextDependencies == 1)
    assert(cell1.numTotalDependencies == 1)
    assert(completer2.cell.numNextDependentCells == 1)
    assert(completer2.cell.numCompleteDependentCells == 1)

    cell1.onNext {
      case Success(x) =>
        assert((x === 8 && !cell1.isComplete) || x === 10)
        latch1.countDown()
      case Failure(e) =>
        assert(false)
        latch1.countDown()
    }

    cell1.onComplete {
      case Success(x) =>
        assert(x === 10)
        latch2.countDown()
      case Failure(e) =>
        assert(false)
        latch2.countDown()
    }

    completer1.putNext(8)
    latch1.await()

    assert(!cell1.isComplete)

    completer2.putFinal(10)
    latch2.await()

    assert(cell1.isComplete)

    pool.shutdown()
  }

  test("put: isFinal == true") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int]("somekey")
    completer.cell.onComplete {
      case Success(v) =>
        assert(v === 6)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    completer.put(6, true)

    latch.await()
    pool.onQuiescenceShutdown()
  }

  test("put: isFinal == false") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int]("somekey")
    completer.cell.onNext {
      case Success(x) =>
        assert(x === 10)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    completer.put(10, false)

    latch.await()
    pool.onQuiescenceShutdown()
  }

  test("putFinal: result passed to callbacks") {
    val latch = new CountDownLatch(1)

    implicit val setLattice = new Lattice[Set[Int]] {
      def join(curr: Set[Int], v2: Set[Int]): Set[Int] = curr ++ v2
      val bottom = Set.empty[Int]
    }
    val key = new lattice.DefaultKey[Set[Int]]

    implicit val pool = new HandlerPool
    val completer = CellCompleter[key.type, Set[Int]](key)
    val cell = completer.cell
    cell.onComplete {
      case Success(v) =>
        assert(v === Set(3, 4, 5))
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    completer.putNext(Set(3, 5))
    completer.putFinal(Set(4))

    latch.await()
    pool.onQuiescenceShutdown()
  }

  test("putNext: result passed to callbacks") {
    val latch = new CountDownLatch(1)

    implicit val setLattice = new Lattice[Set[Int]] {
      def join(curr: Set[Int], v2: Set[Int]): Set[Int] = curr ++ v2
      val bottom = Set.empty[Int]
    }
    val key = new lattice.DefaultKey[Set[Int]]

    implicit val pool = new HandlerPool
    val completer = CellCompleter[key.type, Set[Int]](key)
    val cell = completer.cell
    completer.putNext(Set(3, 5))
    cell.onNext {
      case Success(v) =>
        assert(v === Set(3, 4, 5) || v === Set(3, 5))
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    completer.putNext(Set(4))

    latch.await()
    pool.onQuiescenceShutdown()
  }

  test("handler pool") {
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

  test("quiescent incomplete cells") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("key1")
    val completer2 = CellCompleter[StringIntKey, Int]("key2")
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.whenComplete(cell2, x => if (x == 1) FinalOutcome(1) else NoOutcome)
    cell2.whenComplete(cell1, x => if (x == 1) FinalOutcome(1) else NoOutcome)
    val incompleteFut = pool.quiescentIncompleteCells
    val cells = Await.result(incompleteFut, 2.seconds)
    assert(cells.map(_.key).toList.toString == "List(key1, key2)")
  }

  test("quiescent resolve cycle") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("key1")
    val completer2 = CellCompleter[StringIntKey, Int]("key2")
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.whenComplete(cell2, x => if (x == 0) FinalOutcome(0) else NoOutcome)
    cell2.whenComplete(cell1, x => if (x == 0) FinalOutcome(0) else NoOutcome)
    val qfut = pool.quiescentResolveCell
    Await.ready(qfut, 2.seconds)
    val incompleteFut = pool.quiescentIncompleteCells
    val cells = Await.result(incompleteFut, 2.seconds)
    assert(cells.size == 0)
  }

  test("getResult: from complete cell") {
    implicit val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int]("somekey")
    val cell = completer.cell

    completer.putFinal(10)

    val result = cell.getResult

    assert(result == 10)
  }

  test("getResult: from incomplete cell") {
    implicit val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int]("somekey")
    val cell = completer.cell

    val result = cell.getResult

    assert(result == 0)
  }

  test("getResult: from a partially complete cell") {
    implicit val pool = new HandlerPool
    val completer = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
    val cell = completer.cell

    completer.putNext(ConditionallyImmutable)

    val res = cell.getResult

    assert(res == ConditionallyImmutable)
  }

  //  test("PurityUpdate: successful updated") {
  //    val lattice = Purity.PurityUpdater
  //
  //    val purity = lattice.update(UnknownPurity, Pure)
  //    assert(purity == Pure)
  //
  //    val newPurity = lattice.update(purity, Pure)
  //    assert(newPurity == Pure)
  //  }
  //
  //  test("PurityUpdater: failed updates") {
  //    val lattice = Purity.PurityUpdater
  //
  //    try {
  //      val newPurity = lattice.update(Impure, Pure)
  //      assert(false)
  //    } catch {
  //      case lve: NotMonotonicException[_] => assert(true)
  //      case e: Exception => assert(false)
  //    }
  //
  //    try {
  //      val newPurity = lattice.update(Pure, Impure)
  //      assert(false)
  //    } catch {
  //      case lve: NotMonotonicException[_] => assert(true)
  //      case e: Exception => assert(false)
  //    }
  //  }

  test("putNext: Successful, using ImmutabilityLattce") {
    implicit val pool = new HandlerPool
    val completer = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
    val cell = completer.cell

    completer.putNext(Immutable)
    assert(cell.getResult == Immutable)

    completer.putNext(ConditionallyImmutable)
    assert(cell.getResult == ConditionallyImmutable)

    // Should be allowed because it's the same value and is allowed by the lattice
    completer.putFinal(ConditionallyImmutable)
    assert(cell.getResult == ConditionallyImmutable)

    // Even though cell is completed, this should be allowed because it's the same
    // value and is allowed by the lattice
    completer.putNext(ConditionallyImmutable)
    assert(cell.getResult == ConditionallyImmutable)

    // Even though cell is completed, this should be allowed because it wont add any new information
    completer.putNext(Immutable)
    assert(cell.getResult() == ConditionallyImmutable)

    pool.onQuiescenceShutdown()
  }

  //  test("putNext: Failed, using ImmutabilityLattce") {
  //  The tested behaviour is not wanted any more.
  //    implicit val pool = new HandlerPool
  //
  //    val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)
  //    val cell2 = completer2.cell
  //
  //    completer2.putFinal(Immutable)
  //
  //    // Should fail putNext with IllegalStateException because of adding new information
  //    // to an already complete cell
  //    try {
  //      completer2.putNext(ConditionallyImmutable)
  //      assert(false)
  //    } catch {
  //      case ise: IllegalStateException => assert(true)
  //      case e: Exception => assert(false)
  //    }
  //
  //    pool.onQuiescenceShutdown()
  //  }

  test("putNext: concurrency test") {
    implicit val pool = new HandlerPool

    for (i <- 1 to 10000) {
      val completer = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

      pool.execute(() => completer.putNext(Immutable))
      pool.execute(() => completer.putNext(ConditionallyImmutable))
      pool.execute(() => completer.putNext(Mutable))

      val p = Promise[Boolean]()
      pool.onQuiescent { () => p.success(true) }

      try {
        Await.result(p.future, 30.seconds)
      } catch {
        case t: Throwable => assert(false, s"failure after $i iterations")
      }

      assert(completer.cell.getResult() == Mutable)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNext and whenComplete: same depender") {
    val latch1 = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("c1")
    val completer2 = CellCompleter[StringIntKey, Int]("c2")

    completer2.cell.whenNext(completer1.cell, (v) => {
      FinalOutcome(10)
    })
    completer2.cell.whenComplete(completer1.cell, (v) => {
      FinalOutcome(10)
    })

    completer2.cell.onComplete(v => latch1.countDown())

    completer1.putFinal(10)

    latch1.await()

    pool.onQuiescenceShutdown()

    assert(completer1.cell.getResult() == 10)
    assert(completer2.cell.getResult() == 10)
  }

  test("whenNext and whenComplete: different depender") {
    val latch1 = new CountDownLatch(2)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("c1")
    val completer2 = CellCompleter[StringIntKey, Int]("c2")
    val completer3 = CellCompleter[StringIntKey, Int]("c3")

    completer2.cell.whenNext(completer1.cell, (v) => {
      FinalOutcome(10)
    })
    completer3.cell.whenComplete(completer1.cell, (v) => {
      FinalOutcome(10)
    })

    completer2.cell.onComplete(v => latch1.countDown())
    completer3.cell.onComplete(v => latch1.countDown())

    completer1.putFinal(10)

    latch1.await()

    pool.onQuiescenceShutdown()

    assert(completer1.cell.getResult() == 10)
    assert(completer2.cell.getResult() == 10)
    assert(completer3.cell.getResult() == 10)
  }

  test("putNext and putFinal: concurrency test") {
    implicit val pool = new HandlerPool

    for (i <- 1 to 10000) {
      val completer = CellCompleter[ImmutabilityKey.type, Immutability](ImmutabilityKey)

      pool.execute(() => completer.putNext(Immutable))
      pool.execute(() => completer.putNext(ConditionallyImmutable))
      pool.execute(() => completer.putFinal(Mutable))

      val p = Promise[Boolean]()
      pool.onQuiescent { () => p.success(true) }

      try {
        Await.result(p.future, 2.seconds)
      } catch {
        case t: Throwable => assert(false, s"failure after $i iterations")
      }

      assert(completer.cell.getResult() == Mutable)
    }

    pool.onQuiescenceShutdown()
  }

  test("New ImmutabilityLattice: successful joins") {
    val lattice = Immutability.ImmutabilityLattice

    val mutability1 = lattice.join(Immutable, ConditionallyImmutable)
    assert(mutability1 == ConditionallyImmutable)

    val mutability2 = lattice.join(ConditionallyImmutable, Mutable)
    assert(mutability2 == Mutable)

    val mutability3 = lattice.join(Immutable, Mutable)
    assert(mutability3 == Mutable)

    val mutability4 = lattice.join(ConditionallyImmutable, Immutable)
    assert(mutability4 == ConditionallyImmutable)

    val mutability5 = lattice.join(Mutable, ConditionallyImmutable)
    assert(mutability5 == Mutable)

    val mutability6 = lattice.join(Mutable, Immutable)
    assert(mutability6 == Mutable)
  }

  test("if exception-throwing tasks should still run quiescent handlers") {
    implicit val intMaxLattice: Lattice[Int] = new Lattice[Int] {
      override def join(v1: Int, v2: Int): Int = Math.max(v1, v2)
      val bottom = 0
    }
    val key = new lattice.DefaultKey[Int]

    implicit val pool = new HandlerPool(unhandledExceptionHandler = { t => /* do nothing */ })
    val completer = CellCompleter[key.type, Int](key)

    pool.execute { () =>
      // NOTE: This will print a stacktrace, but that is fine (not a bug).
      throw new Exception(
        "Even if this happens, quiescent handlers should still run.")
    }

    try {
      Await.result(pool.quiescentIncompleteCells, 1.seconds)
    } catch {
      case _: java.util.concurrent.TimeoutException => assert(false)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNext: cSCC with constant resolution") {
    val latch = new CountDownLatch(4)

    implicit val pool = new HandlerPool

    val completer1 = CellCompleter[StringIntKey, Int]("somekey1")
    val cell1 = completer1.cell
    val completer2 = CellCompleter[StringIntKey, Int]("somekey2")
    val cell2 = completer2.cell
    val completer3 = CellCompleter[StringIntKey, Int]("somekey3")
    val cell3 = completer3.cell
    val completer4 = CellCompleter[StringIntKey, Int]("somekey4")
    val cell4 = completer4.cell

    // set unwanted values:
    completer1.putNext(-1)
    completer2.putNext(-1)
    completer3.putNext(-1)
    completer4.putNext(-1)

    // create a cSCC, assert that none of the callbacks get called again.
    cell1.whenNext(cell2, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell1.whenNext(cell3, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell2.whenNext(cell4, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell3.whenNext(cell4, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell4.whenNext(cell1, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)

    for (c <- List(cell1, cell2, cell3, cell4))
      c.onComplete {
        case Success(v) =>
          assert(v === 0)
          assert(c.numNextDependencies === 0)
          latch.countDown()
        case Failure(e) =>
          assert(false)
          latch.countDown()
      }

    pool.triggerExecution(cell1)

    // resolve cells
    val fut = pool.quiescentResolveCell
    Await.result(fut, 2.seconds)
    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: cSCC with constant resolution") {
    val latch = new CountDownLatch(4)

    implicit val pool = new HandlerPool

    val completer1 = CellCompleter[StringIntKey, Int]("somekey1")
    val cell1 = completer1.cell
    val completer2 = CellCompleter[StringIntKey, Int]("somekey2")
    val cell2 = completer2.cell
    val completer3 = CellCompleter[StringIntKey, Int]("somekey3")
    val cell3 = completer3.cell
    val completer4 = CellCompleter[StringIntKey, Int]("somekey4")
    val cell4 = completer4.cell

    // set unwanted values:
    completer1.putNext(-1)
    completer2.putNext(-1)
    completer3.putNext(-1)
    completer4.putNext(-1)

    // create a cSCC, assert that none of the callbacks get called.
    cell1.whenNextSequential(cell2, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell1.whenNextSequential(cell3, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell2.whenNextSequential(cell4, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell3.whenNextSequential(cell4, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell4.whenNextSequential(cell1, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)

    for (c <- List(cell1, cell2, cell3, cell4))
      c.onComplete {
        case Success(v) =>
          assert(v === 0)
          assert(c.numNextDependencies === 0)
          latch.countDown()
        case Failure(e) =>
          assert(false)
          latch.countDown()
      }

    pool.triggerExecution(cell1)

    // resolve cells
    val fut = pool.quiescentResolveCell
    Await.result(fut, 2.seconds)
    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNext: cSCC with default resolution") {
    val latch = new CountDownLatch(4)

    implicit val pool = new HandlerPool

    val completer1 = CellCompleter[StringIntKey, Int]("somekey1")
    val cell1 = completer1.cell
    val completer2 = CellCompleter[StringIntKey, Int]("somekey2")
    val cell2 = completer2.cell
    val completer3 = CellCompleter[StringIntKey, Int]("somekey3")
    val cell3 = completer3.cell
    val completer4 = CellCompleter[StringIntKey, Int]("somekey4")
    val cell4 = completer4.cell

    // set unwanted values:
    completer1.putNext(-1)
    completer2.putNext(-1)
    completer3.putNext(-1)
    completer4.putNext(-1)

    // create a cSCC, assert that none of the callbacks get called again.
    cell1.whenNext(cell2, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell1.whenNext(cell3, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell2.whenNext(cell4, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell3.whenNext(cell4, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell4.whenNext(cell1, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)

    for (c <- List(cell1, cell2, cell3, cell4))
      c.onComplete {
        case Success(v) =>
          assert(v === 1)
          assert(c.numNextDependencies === 0)
          latch.countDown()
        case Failure(e) =>
          assert(false)
          latch.countDown()
      }

    // resolve cells
    pool.whileQuiescentResolveDefault
    val fut = pool.quiescentResolveDefaults
    Await.result(fut, 2.second)
    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: cSCC with default resolution") {
    val latch = new CountDownLatch(4)

    implicit val pool = new HandlerPool

    val completer1 = CellCompleter[StringIntKey, Int]("somekey1")
    val cell1 = completer1.cell
    val completer2 = CellCompleter[StringIntKey, Int]("somekey2")
    val cell2 = completer2.cell
    val completer3 = CellCompleter[StringIntKey, Int]("somekey3")
    val cell3 = completer3.cell
    val completer4 = CellCompleter[StringIntKey, Int]("somekey4")
    val cell4 = completer4.cell

    // set unwanted values:
    completer1.putNext(-1)
    completer2.putNext(-1)
    completer3.putNext(-1)
    completer4.putNext(-1)

    // create a cSCC, assert that none of the callbacks get called.
    cell1.whenNextSequential(cell2, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell1.whenNextSequential(cell3, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell2.whenNextSequential(cell4, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell3.whenNextSequential(cell4, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)
    cell4.whenNextSequential(cell1, v => if (v != -1) { assert(false); NextOutcome(-2) } else NoOutcome)

    for (c <- List(cell1, cell2, cell3, cell4))
      c.onComplete {
        case Success(v) =>
          assert(v === 1)
          assert(c.numNextDependencies === 0)
          latch.countDown()
        case Failure(e) =>
          assert(false)
          latch.countDown()
      }

    // resolve cells
    pool.whileQuiescentResolveDefault
    val fut = pool.quiescentResolveDefaults
    Await.result(fut, 2.second)
    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("whenNext: cycle with default resolution") {
    sealed trait Value
    case object Bottom extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val initial: Value = Bottom
    }

    implicit val pool = new HandlerPool

    for (i <- 1 to 100) {
      val completer1 = CellCompleter[DefaultKey[Value], Value](new DefaultKey[Value])
      val completer2 = CellCompleter[DefaultKey[Value], Value](new DefaultKey[Value])
      val cell1 = completer1.cell
      val cell2 = completer2.cell

      cell1.whenNext(cell2, v => NextOutcome(ShouldNotHappen))
      cell2.whenNext(cell1, v => NextOutcome(ShouldNotHappen))

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 1.minutes)

      assert(cell1.getResult() != ShouldNotHappen)
      assert(cell2.getResult() != ShouldNotHappen)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: cycle with default resolution") {
    sealed trait Value
    case object Bottom extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val initial: Value = Bottom
    }

    implicit val pool = new HandlerPool

    for (i <- 1 to 100) {
      val completer1 = CellCompleter[DefaultKey[Value], Value](new DefaultKey[Value])
      val completer2 = CellCompleter[DefaultKey[Value], Value](new DefaultKey[Value])
      val cell1 = completer1.cell
      val cell2 = completer2.cell

      cell1.whenNextSequential(cell2, v => NextOutcome(ShouldNotHappen))
      cell2.whenNextSequential(cell1, v => NextOutcome(ShouldNotHappen))

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 1.minutes)

      assert(cell1.getResult() != ShouldNotHappen)
      assert(cell2.getResult() != ShouldNotHappen)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNext: cycle with constant resolution") {
    sealed trait Value
    case object Bottom extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = if (v1 == Bottom) v2 else v1 // TODO or throw?
      override val initial: Value = Bottom
    }

    object TheKey extends DefaultKey[Value] {
      override def resolve[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, OK))
      }
    }

    implicit val pool = new HandlerPool

    for (i <- 1 to 100) {
      val completer1 = CellCompleter[TheKey.type, Value](TheKey)
      val completer2 = CellCompleter[TheKey.type, Value](TheKey)
      val cell1 = completer1.cell
      val cell2 = completer2.cell

      cell1.whenNext(cell2, v => NextOutcome(ShouldNotHappen))
      cell2.whenNext(cell1, v => NextOutcome(ShouldNotHappen))

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 1.minutes)

      assert(cell1.getResult() == OK)
      assert(cell2.getResult() == OK)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: cycle with constant resolution") {
    sealed trait Value
    case object Bottom extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val initial: Value = Bottom
    }

    object TheKey extends DefaultKey[Value] {
      override def resolve[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, OK))
      }
    }

    implicit val pool = new HandlerPool

    for (i <- 1 to 100) {
      val completer1 = CellCompleter[TheKey.type, Value](TheKey)
      val completer2 = CellCompleter[TheKey.type, Value](TheKey)
      val cell1 = completer1.cell
      val cell2 = completer2.cell

      cell1.whenNextSequential(cell2, v => NextOutcome(ShouldNotHappen))
      cell2.whenNextSequential(cell1, v => NextOutcome(ShouldNotHappen))

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 1.minutes)

      assert(cell1.getResult() == OK)
      assert(cell2.getResult() == OK)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNext: cycle with additional incoming dep") {
    sealed trait Value
    case object Bottom extends Value
    case object Resolved extends Value
    case object Fallback extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val initial: Value = Bottom
    }

    object TheKey extends DefaultKey[Value] {
      override def resolve[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, Fallback))
      }
    }

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[TheKey.type, Value](TheKey)
    val completer2 = CellCompleter[TheKey.type, Value](TheKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val in = CellCompleter[TheKey.type, Value](TheKey)

    // let `cell1` and `cell2` form a cycle
    cell1.whenNext(cell2, v => NextOutcome(ShouldNotHappen))
    cell2.whenNext(cell1, v => NextOutcome(ShouldNotHappen))

    // the cycle is dependent on incoming information from `in`
    cell2.whenNext(in.cell, v => { NextOutcome(ShouldNotHappen) })

    // resolve the independent cell `in` and the cycle
    val fut = pool.quiescentResolveCell
    Await.ready(fut, 1.minutes)

    pool.onQuiescenceShutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(in.cell.getResult() == Fallback)
  }

  test("whenNextSequential: cycle with additional incoming dep") {
    sealed trait Value
    case object Bottom extends Value
    case object Resolved extends Value
    case object Fallback extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val initial: Value = Bottom
    }

    object TheKey extends DefaultKey[Value] {
      override def resolve[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, Fallback))
      }
    }

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[TheKey.type, Value](TheKey)
    val completer2 = CellCompleter[TheKey.type, Value](TheKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val in = CellCompleter[TheKey.type, Value](TheKey)

    // let `cell1` and `cell2` form a cycle
    cell1.whenNextSequential(cell2, v => NextOutcome(ShouldNotHappen))
    cell2.whenNextSequential(cell1, v => NextOutcome(ShouldNotHappen))

    // the cycle is dependent on incoming information from `in`
    cell2.whenNextSequential(in.cell, v => { NextOutcome(ShouldNotHappen) })

    // resolve the independent cell `in` and the cycle
    val fut = pool.quiescentResolveCell
    Await.ready(fut, 1.minutes)

    pool.shutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(in.cell.getResult() == Fallback)
  }

  test("whenComplete: cycle with additional incoming dep") {
    sealed trait Value
    case object Bottom extends Value
    case object Resolved extends Value
    case object Fallback extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val initial: Value = Bottom
    }

    object TheKey extends DefaultKey[Value] {
      override def resolve[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, Fallback))
      }
    }

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[TheKey.type, Value](TheKey)
    val completer2 = CellCompleter[TheKey.type, Value](TheKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val in = CellCompleter[TheKey.type, Value](TheKey)

    // let `cell1` and `cell2` form a cycle
    cell1.whenComplete(cell2, v => NextOutcome(ShouldNotHappen))
    cell2.whenComplete(cell1, v => NextOutcome(ShouldNotHappen))

    // the cycle is dependent on incoming information from `in`
    cell2.whenComplete(in.cell, v => { NextOutcome(ShouldNotHappen) })

    // resolve the independent cell `in` and the cycle
    val fut = pool.quiescentResolveCell
    Await.ready(fut, 1.minutes)

    pool.onQuiescenceShutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(in.cell.getResult() == Fallback)
  }

  test("whenNext: cycle with additional outgoing dep") {
    sealed trait Value
    case object Bottom extends Value
    case object Dummy extends Value
    case object Resolved extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val initial: Value = Bottom
    }

    object TheKey extends DefaultKey[Value] {
      override def resolve[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        Seq()
      }
    }

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[TheKey.type, Value](TheKey)
    val completer2 = CellCompleter[TheKey.type, Value](TheKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val out = CellCompleter[TheKey.type, Value](TheKey)
    out.putNext(Dummy)
    cell1.whenNext(cell2, v => NextOutcome(ShouldNotHappen))
    cell2.whenNext(cell1, v => NextOutcome(ShouldNotHappen))
    out.putNext(ShouldNotHappen)
    out.cell.whenComplete(cell1, v => FinalOutcome(OK))

    val fut = pool.quiescentResolveCycles
    Await.ready(fut, 1.minutes)

    pool.onQuiescenceShutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(out.cell.getResult() == OK)
  }

  test("whenNextSequential: cycle with additional outgoing dep") {
    sealed trait Value
    case object Bottom extends Value
    case object Dummy extends Value
    case object Resolved extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val initial: Value = Bottom
    }

    object TheKey extends DefaultKey[Value] {
      override def resolve[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        Seq()
      }
    }

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[TheKey.type, Value](TheKey)
    val completer2 = CellCompleter[TheKey.type, Value](TheKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val out = CellCompleter[TheKey.type, Value](TheKey)
    out.putNext(Dummy)
    cell1.whenNextSequential(cell2, v => NextOutcome(ShouldNotHappen))
    cell2.whenNextSequential(cell1, v => NextOutcome(ShouldNotHappen))
    out.putNext(ShouldNotHappen)
    out.cell.whenComplete(cell1, v => FinalOutcome(OK))

    val fut = pool.quiescentResolveCycles
    Await.ready(fut, 1.minutes)

    pool.onQuiescenceShutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(out.cell.getResult() == OK)
  }

  test("whenCompleteSequential: calling sequentially") {
    val n = 1000

    val runningCallbacks = new AtomicInteger(0)
    val latch = new CountDownLatch(1)
    val random = new scala.util.Random()

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[lattice.NaturalNumberKey.type, Int](lattice.NaturalNumberKey)

    val cell1 = completer1.cell
    for (i <- 1 to n) { // create n predecessors
      val tmpCompleter = CellCompleter[lattice.NaturalNumberKey.type, Int](lattice.NaturalNumberKey)

      // let cell1 depend on the predecessor tmpCompleter
      cell1.whenCompleteSequential(tmpCompleter.cell, (x: Int) => {
        assert(runningCallbacks.incrementAndGet() == 1)
        Thread.`yield`()
        try {
          Thread.sleep(random.nextInt(3))
        } catch {
          case _: InterruptedException => /* ignore */
        }
        assert(runningCallbacks.decrementAndGet() == 0)
        Outcome(x * n, x == n)
      })

      cell1.onComplete(_ => {
        latch.countDown()
      })

      pool.execute(() => tmpCompleter.putFinal(i))
    }

    assert(latch.await(10, TimeUnit.SECONDS))

    assert(cell1.getResult() == n * n)

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: calling sequentially") {
    val n = 1000

    val runningCallbacks = new AtomicInteger(0)
    val latch = new CountDownLatch(1)
    val random = new scala.util.Random()

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[lattice.NaturalNumberKey.type, Int](lattice.NaturalNumberKey)(Updater.latticeToUpdater(new NaturalNumberLattice), pool)
    val completer2 = CellCompleter[lattice.NaturalNumberKey.type, Int](lattice.NaturalNumberKey)(Updater.latticeToUpdater(new NaturalNumberLattice), pool)

    val cell1 = completer1.cell
    cell1.whenNextSequential(completer2.cell, (x: Int) => {
      assert(runningCallbacks.incrementAndGet() == 1)
      Thread.`yield`()
      try {
        Thread.sleep(random.nextInt(3))
      } catch {
        case _: InterruptedException => /* ignore */
      }
      assert(runningCallbacks.decrementAndGet() == 0)
      Outcome(x * n, x == n)
    })

    cell1.onComplete(_ => {
      latch.countDown()
    })

    for (i <- 1 to n)
      pool.execute(() => completer2.putNext(i))

    latch.await()

    assert(cell1.getResult() == n * n)
    assert(completer2.cell.getResult() == n)

    pool.onQuiescenceShutdown()
  }

  test("whenSequential: calling sequentially") {
    val n = 1000

    val runningCallbacks = new AtomicInteger(0)
    val latch = new CountDownLatch(1)
    val random = new scala.util.Random()

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[lattice.NaturalNumberKey.type, Int](lattice.NaturalNumberKey)(Updater.latticeToUpdater(new NaturalNumberLattice), pool)
    val completer2 = CellCompleter[lattice.NaturalNumberKey.type, Int](lattice.NaturalNumberKey)(Updater.latticeToUpdater(new NaturalNumberLattice), pool)

    val cell1 = completer1.cell
    cell1.whenSequential(completer2.cell, (x: Int, _) => {
      assert(runningCallbacks.incrementAndGet() == 1)
      Thread.`yield`()
      try {
        Thread.sleep(random.nextInt(3))
      } catch {
        case _: InterruptedException => /* ignore */
      }
      assert(runningCallbacks.decrementAndGet() == 0)
      Outcome(x * n, x == n)
    })

    cell1.onComplete(_ => {
      latch.countDown()
    })

    for (i <- 1 to n)
      pool.execute(() => completer2.putNext(i))

    latch.await()

    assert(cell1.getResult() == n * n)
    assert(completer2.cell.getResult() == n)

    pool.onQuiescenceShutdown()
  }

  // TODO I need to think of a better way to test working with a state.
  //  As is, the tests checks, if n steps are taken, but this is
  //  not true any more, as the dependentCell might pull, after
  //  the staged values potentially have been updated several times.
  //  test("whenNextSequential: state") {
  //    val n = 1000
  //    var count = 0
  //
  //    val runningCallbacks = new AtomicInteger(0)
  //    val latch = new CountDownLatch(1)
  //    val random = new scala.util.Random()
  //
  //    implicit val pool = new HandlerPool
  //    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
  //    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")
  //
  //    val cell1 = completer1.cell
  //    cell1.whenNextSequential(completer2.cell, (x: Int) => {
  //      assert(runningCallbacks.incrementAndGet() == 1)
  //      count += 1
  //      Thread.`yield`()
  //      try {
  //        Thread.sleep(random.nextInt(3))
  //      } catch {
  //        case _: InterruptedException => /* ignore */
  //      }
  //      assert(runningCallbacks.decrementAndGet() == 0)
  //      Outcome(count, count == n)
  //    })
  //
  //    cell1.onComplete(_ => {
  //      latch.countDown()
  //    })
  //
  //    for (i <- 1 to n)
  //      pool.execute(() => completer2.putNext(i))
  //
  //    latch.await()
  //
  //    assert(cell1.getResult() == n)
  //
  //    pool.onQuiescenceShutdown()
  //  }
  //
  //  test("whenCompleteSequential: state") {
  //    val n = 1000
  //    var count = 0
  //
  //    val runningCallbacks = new AtomicInteger(0)
  //    val latch = new CountDownLatch(1)
  //    var otherLatches: Set[CountDownLatch] = Set.empty
  //    val random = new scala.util.Random()
  //
  //    implicit val pool = new HandlerPool
  //    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
  //    val cell1 = completer1.cell
  //
  //    for (i <- 1 to n) {
  //      val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")
  //
  //      val latch2 = new CountDownLatch(1)
  //      otherLatches = otherLatches + latch2
  //
  //      completer2.cell.onComplete(_ => {
  //        latch2.countDown()
  //      })
  //
  //      cell1.whenNextSequential(completer2.cell, (x: Int) => {
  //        assert(runningCallbacks.incrementAndGet() == 1)
  //        count += 1
  //        Thread.`yield`()
  //        try {
  //          Thread.sleep(random.nextInt(3))
  //        } catch {
  //          case _: InterruptedException => /* ignore */
  //        }
  //        assert(runningCallbacks.decrementAndGet() == 0)
  //        Outcome(count, count == n)
  //      })
  //
  //      cell1.onComplete(_ => {
  //        latch.countDown()
  //      })
  //
  //      pool.execute(() => completer2.putFinal(i))
  //
  //    }
  //
  //    latch.await()
  //    otherLatches.foreach(_.await())
  //
  //    assert(cell1.getResult() == n)
  //
  //    pool.onQuiescenceShutdown()
  //  }

  test("whenComplete: called at most once") {
    implicit val intMaxLattice: Lattice[Int] = new Lattice[Int] {
      override def join(x: Int, y: Int): Int = Math.max(x, y)
      val bottom = 0
    }
    val key = new lattice.DefaultKey[Int]

    implicit val pool = new HandlerPool

    for (_ <- 1 to 100) {
      val latch = new CountDownLatch(1)
      val completer1 = CellCompleter[key.type, Int](key)
      val completer2 = CellCompleter[key.type, Int](key)
      var called = 0
      completer2.cell.whenComplete(completer1.cell, _ => {
        assert(called === 0)
        called += 1
        latch.countDown()
        NoOutcome
      })

      // complete cell
      completer1.putFinal(5)

      // put a higher value
      try completer1.putFinal(6) catch {
        case _: IllegalStateException => /* ignore */
      }

      // put a lower value
      try completer1.putFinal(4) catch {
        case _: IllegalStateException => /* ignore */
      }

      // put the same value
      try completer1.putFinal(5) catch {
        case _: IllegalStateException => /* ignore */
      }

      latch.await()

      assert(called === 1)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNextSequential: discard callbacks on completion") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    val n = 10000

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[lattice.NaturalNumberKey.type, Int](lattice.NaturalNumberKey)
    val completer2 = CellCompleter[lattice.NaturalNumberKey.type, Int](lattice.NaturalNumberKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.trigger()

    cell1.whenNextSequential(cell2, v => {
      latch1.await() // wait for some puts/triggers
      FinalOutcome(n)
    })

    for (i <- 1 to n)
      pool.execute(() => completer2.putNext(i))
    latch1.countDown()

    pool.onQuiescent(() => {
      pool.onQuiescenceShutdown()
      latch2.countDown()
    })
    // pool needs to reach quiescence, even if cell1 is completed early:
    latch2.await()

    assert(cell1.getResult() == n)
    assert(cell1.isComplete)
    assert(!cell2.isComplete)
  }

  test("cell dependency on itself") {
    class ReactivePropertyStoreKey extends Key[Int] {
      override def resolve[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] = {
        Seq((cells.head, 42))
      }

      override def fallback[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] = {
        cells.map(cell ⇒ (cell, cell.getResult()))
      }

      override def toString = "ReactivePropertyStoreKey"
    }

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
    cell20.whenNext(cell10, x => {
      if (x == 10) {
        completer20.putFinal(43)
      }
      NoOutcome
    })

    completer1.putNext(10)
    completer10.putNext(10)

    cell1.whenNext(cell1, _ => {
      NoOutcome
    })

    cell1.trigger()
    cell2.trigger()
    cell10.trigger()
    cell20.trigger()

    val fut = pool.quiescentResolveCycles
    Await.ready(fut, 2.seconds)

    val fut2 = pool.quiescentResolveDefaults
    Await.ready(fut2, 2.seconds)

  }

  test("whenCompleteSequential: discard callbacks on completion") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    implicit val pool = new HandlerPool

    val completer1 = CellCompleter[lattice.NaturalNumberKey.type, Int](lattice.NaturalNumberKey)
    val completer2 = CellCompleter[lattice.NaturalNumberKey.type, Int](lattice.NaturalNumberKey)
    val completer3 = CellCompleter[lattice.NaturalNumberKey.type, Int](lattice.NaturalNumberKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val cell3 = completer3.cell
    cell1.trigger()

    cell1.whenCompleteSequential(cell2, v => {
      latch1.await() // wait for some puts/triggers
      FinalOutcome(10)
    })
    cell1.whenCompleteSequential(cell3, NextOutcome(_))

    completer2.putFinal(3)
    completer3.putNext(2)
    completer3.putNext(3)
    latch1.countDown()

    pool.onQuiescent(() => {
      pool.onQuiescenceShutdown()
      latch2.countDown()
    })
    // pool needs to reach quiescence, even if cell1 is completed early:
    latch2.await()

    assert(cell1.getResult() == 10)
    assert(cell2.getResult() == 3)
    assert(cell3.getResult() == 3)
    assert(cell1.isComplete)
    assert(cell2.isComplete)
    assert(!cell3.isComplete)
  }

  test("recursive quiescentResolveCycles") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[RecursiveQuiescentTestKey, Int](new RecursiveQuiescentTestKey)
    val completer2 = CellCompleter[RecursiveQuiescentTestKey, Int](new RecursiveQuiescentTestKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell

    cell1.whenNext(cell1, x => {
      Thread.sleep(200)
      NoOutcome
    })

    cell2.whenNext(cell1, x => {
      Thread.sleep(200)
      FinalOutcome(x * 2)
    })

    val fut = pool.quiescentResolveCycles
    Await.ready(fut, 2.seconds)

    assert(completer1.cell.getResult == 42)
    assert(completer2.cell.getResult == 84)

    pool.onQuiescenceShutdown()
  }

  test("recursive quiescentResolveDefaults") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[RecursiveQuiescentTestKey, Int](new RecursiveQuiescentTestKey)
    val completer2 = CellCompleter[RecursiveQuiescentTestKey, Int](new RecursiveQuiescentTestKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell

    cell2.whenNext(cell1, x => {
      Thread.sleep(200)
      FinalOutcome(x * 2)
    })

    val fut = pool.quiescentResolveDefaults
    Await.ready(fut, 2.seconds)

    assert(completer2.cell.getResult == 86)

    pool.onQuiescenceShutdown()
  }

  test("recursive quiescentResolveCell using resolve") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[RecursiveQuiescentTestKey, Int](new RecursiveQuiescentTestKey)
    val completer2 = CellCompleter[RecursiveQuiescentTestKey, Int](new RecursiveQuiescentTestKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell2.trigger()

    cell1.whenNext(cell1, x => {
      Thread.sleep(200)
      NoOutcome
    })

    cell2.whenNext(cell1, x => {
      Thread.sleep(200)
      FinalOutcome(x * 2)
    })

    val fut = pool.quiescentResolveCell
    Await.ready(fut, 2.seconds)

    assert(completer1.cell.getResult == 42)
    assert(completer2.cell.getResult == 84)

    pool.onQuiescenceShutdown()
  }

  test("recursive quiescentResolveCell using fallback") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[RecursiveQuiescentTestKey, Int](new RecursiveQuiescentTestKey)
    val completer2 = CellCompleter[RecursiveQuiescentTestKey, Int](new RecursiveQuiescentTestKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell

    cell1.whenNext(cell2, x => {
      Thread.sleep(200)
      FinalOutcome(x * 2)
    })

    val fut = pool.quiescentResolveCell
    Await.ready(fut, 2.seconds)

    assert(completer1.cell.getResult == 86)
    assert(completer2.cell.getResult == 43)

    pool.onQuiescenceShutdown()
  }

  class RecursiveQuiescentTestKey extends Key[Int] {
    override def resolve[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] = {
      Seq((cells.head, 42))
    }

    override def fallback[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] = {
      cells.map(cell ⇒ (cell, 43))
    }
  }

  test("cell isADependee") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[RecursiveQuiescentTestKey, Int](new RecursiveQuiescentTestKey)
    val completer2 = CellCompleter[RecursiveQuiescentTestKey, Int](new RecursiveQuiescentTestKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell

    cell1.whenNext(cell2, x => {
      FinalOutcome(1)
    })

    assert(cell2.isADependee())
    assert(!cell1.isADependee())

    pool.shutdown()
  }

}
