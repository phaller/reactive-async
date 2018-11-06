package com.phaller.rasync
package test

import org.scalatest.FunSuite
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import scala.concurrent.duration._

import com.phaller.rasync.cell._

import scala.util.{ Failure, Success, Try }
import scala.concurrent.{ Await, Promise }
import com.phaller.rasync.lattice._
import com.phaller.rasync.pool.HandlerPool
import com.phaller.rasync.test.lattice.IntUpdater

// Tests for puts and non-cyclic deps of the same type
abstract class BaseSuite extends FunSuite with CompleterFactory {

  implicit val intUpdater: Updater[Int] = new IntUpdater

  def if10thenFinal20(updates: Iterable[(Cell[Int], Try[ValueOutcome[Int]])]): Outcome[Int] =
    ifXthenFinalY(10, 20)(updates)

  def ifXthenFinalY(x: Int, y: Int)(upd: Iterable[(Cell[Int], Try[ValueOutcome[Int]])]): Outcome[Int] = {
    val c = upd.head._2
    if (c.get.value == x) FinalOutcome(y) else NoOutcome
  }

  test("putFinal") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int]
    val completer = mkCompleter[Int]
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
    implicit val pool = new HandlerPool[Int]
    val completer = mkCompleter[Int]
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
    implicit val pool = new HandlerPool[Immutability]
    val completer = mkCompleter[Immutability]
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

  test("when") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int](parallelism = 1)
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(if10thenFinal20)

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

  test("when: dependency count after putFinal") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(if10thenFinal20)

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

    assert(cell1.numDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("when: dependency count 2") {
    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(if10thenFinal20)

    completer2.putFinal(9)

    cell1.waitUntilNoDeps(2, TimeUnit.SECONDS)

    assert(cell1.numDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("when: dependency count for multiple deps") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]
    val completer3 = mkCompleter[Int]
    val completer4 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(if10thenFinal20)
    cell1.when(completer2.cell, completer3.cell)(ifXthenFinalY(20, 30)) // cell2 should be ignored

    assert(cell1.numDependencies == 2)

    pool.shutdown()
  }

  test("when: dependency count for multiple deps 2") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]
    val completer3 = mkCompleter[Int]
    val completer4 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(if10thenFinal20)
    cell1.when(completer2.cell)(ifXthenFinalY(20, 30)) // should be ignored

    cell1.when(completer3.cell, completer4.cell)(if10thenFinal20)
    cell1.when(completer3.cell)(ifXthenFinalY(20, 30)) // should be ignored
    cell1.when(completer4.cell)(ifXthenFinalY(20, 30)) // should be ignored

    assert(cell1.numDependencies == 3)

    pool.shutdown()
  }

  test("when: callback removal") {
    implicit val pool = new HandlerPool[Int]
    val latch = new CountDownLatch(1)

    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]

    completer1.cell.when(completer2.cell)(if10thenFinal20)

    completer1.cell.onComplete(_ => latch.countDown())

    completer2.putFinal(10)

    latch.await()

    assert(completer2.cell.numDependentCells == 0)
    completer2.putFinal(30) // this should be ignored

    assert(completer1.cell.getResult() == 20)
    assert(completer2.cell.getResult() == 10)

    pool.onQuiescenceShutdown()
  }

  test("onNext") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int]
    val completer = mkCompleter[Int]

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

  test("when: num dependencies after non-final value") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(if10thenFinal20)

    assert(cell1.numDependencies == 1)
    assert(completer2.cell.numDependentCells == 1)

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

    assert(cell1.numDependencies == 0)
    assert(completer2.cell.numDependentCells == 0)

    pool.onQuiescenceShutdown()
  }

  test("whenNext: Triggered by putFinal") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(if10thenFinal20)
    assert(cell1.numDependencies == 1)

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

  test("when: Remove dependencies from cells that depend on a completing cell") {
    /* Needs the dependees from the feature/cscc-resolving branch */
    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(if10thenFinal20)

    completer2.putFinal(10)

    cell1.waitUntilNoDeps()

    assert(cell1.numDependencies == 0)

    pool.onQuiescenceShutdown()
  }

  test("when: callback removal 2") {
    implicit val pool = new HandlerPool[Immutability]
    val completer1 = mkCompleter[Immutability]
    val completer2 = mkCompleter[Immutability]

    completer1.cell.when(completer2.cell)(_.head._2 match {
      case Success(NextOutcome(Mutable)) => NextOutcome(Mutable)
      case _ => NoOutcome
    })

    completer1.putFinal(Immutable)
    assert(completer2.cell.numDependentCells == 0)
    completer2.putNext(Mutable)

    assert(completer1.cell.getResult() == Immutable)
    assert(completer2.cell.getResult() == Mutable)

    pool.onQuiescenceShutdown()
  }

  test("when: Dependencies concurrency test") {
    val n = 10000
    implicit val pool = new HandlerPool[Immutability]
    val latch = new CountDownLatch(n)
    val completer1 = mkCompleter[Immutability]

    for (i <- 1 to n) {
      pool.execute(() => {
        val completer2 = mkCompleter[Immutability]
        val completer3 = mkCompleter[Immutability]
        completer1.cell.when(completer2.cell, completer3.cell)(_ => NextOutcome(Mutable))
        latch.countDown()
      })
    }

    latch.await()

    assert(completer1.cell.numDependencies == 2 * n)

    pool.onQuiescenceShutdown()
  }

  test("when: concurrent put final") {
    var expectedValue: Option[Immutability] = None

    for (_ <- 1 to 100) {
      implicit val pool = new HandlerPool[Immutability]
      val completer1 = mkCompleter[Immutability]
      val completer2 = mkCompleter[Immutability]

      val cell1 = completer1.cell
      cell1.trigger()

      pool.execute(() => cell1.when(completer2.cell)(_ => {
        NoOutcome
      }))

      pool.execute(() => completer2.putFinal(Mutable))

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 2.seconds)

      if (expectedValue.isEmpty) expectedValue = Some(cell1.getResult())
      else assert(cell1.getResult() == expectedValue.get)

      pool.onQuiescenceShutdown()
    }
  }

  test("when: One cell with several dependencies on the same cell concurrency test") {
    implicit val pool = new HandlerPool[Immutability]()

    for (_ <- 1 to 1000) {
      val completer1 = mkCompleter[Immutability]
      val completer2 = mkCompleter[Immutability]
      completer1.cell.when(completer2.cell)(it => it.head._2.get.value match {
        case Immutable | ConditionallyImmutable => NoOutcome
        case Mutable => NextOutcome(Mutable)
      })

      assert(completer1.cell.numDependencies == 1)

      pool.execute(() => completer2.putNext(ConditionallyImmutable))
      pool.execute(() => completer2.putFinal(Mutable))

      val fut = pool.quiescentResolveCell
      Await.result(fut, 2.second)

      assert(completer2.cell.getResult() == Mutable)
      assert(completer1.cell.getResult() == Mutable)
    }

    pool.onQuiescenceShutdown()
  }

  test("when: concurrent put next") {
    var expectedValue: Option[Immutability] = None

    for (_ <- 1 to 100) {
      implicit val pool = new HandlerPool[Immutability]()
      val completer1 = mkCompleter[Immutability]
      val completer2 = mkCompleter[Immutability]

      val cell1 = completer1.cell

      pool.execute(() => cell1.when(completer2.cell)(it => {
        if (it.head._2.get.value == Mutable) NextOutcome(Mutable)
        else NoOutcome
      }))
      pool.execute(() => completer2.putNext(Mutable))

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 2.seconds)

      if (expectedValue.isEmpty) expectedValue = Some(cell1.getResult())
      else assert(cell1.getResult() == expectedValue.get)

      pool.shutdown()
    }
  }

  test("when: complete dependent cell - ignored update") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(if10thenFinal20)

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
      case _: IllegalStateException => assert(false)
      case _: Exception => assert(false)
    }

    pool.onQuiescenceShutdown()
  }

  test("when: complete depedent cell, dependency 1") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(if10thenFinal20)

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

  test("when: complete dependent cell, dependency 2") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(if10thenFinal20)

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

    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]

    val cell1 = completer1.cell
    cell1.when(completer2.cell)(it => {
      val x = it.head._2
      x.get match {
        case FinalOutcome(_) => x.get // complete, if completer2 is completed
        case _ => NoOutcome
      }
    })

    assert(cell1.numDependencies == 1)

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

  test("put: isFinal == true") {
    val latch = new CountDownLatch(1)

    implicit val pool = new HandlerPool[Int]
    val completer = mkCompleter[Int]
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

    implicit val pool = new HandlerPool[Int]
    val completer = mkCompleter[Int]
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
    val key = new DefaultKey[Set[Int]]

    implicit val pool = new HandlerPool[Set[Int]]
    val completer = mkCompleter[Set[Int]]
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
    val key = new DefaultKey[Set[Int]]

    implicit val pool = new HandlerPool[Set[Int]]
    val completer = mkCompleter[Set[Int]]
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

  test("quiescent incomplete cells") {
    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.when(cell2)(if10thenFinal20)
    cell2.when(cell1)(if10thenFinal20)
    val incompleteFut = pool.quiescentIncompleteCells
    val cells = Await.result(incompleteFut, 2.seconds)
    assert(cells.size == 2)
  }

  test("quiescent resolve cycle") {
    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.when(cell2)(_ => NoOutcome)
    cell2.when(cell1)(_ => NoOutcome)
    val qfut = pool.quiescentResolveCell
    Await.ready(qfut, 2.seconds)
    val incompleteFut = pool.quiescentIncompleteCells
    val cells = Await.result(incompleteFut, 2.seconds)
    assert(cells.isEmpty)
  }

  test("getResult: from complete cell") {
    implicit val pool = new HandlerPool[Int]
    val completer = mkCompleter[Int]
    val cell = completer.cell

    completer.putFinal(10)

    val result = cell.getResult

    assert(result == 10)
  }

  test("getResult: from incomplete cell") {
    implicit val pool = new HandlerPool[Int]
    val completer = mkCompleter[Int]
    val cell = completer.cell

    val result = cell.getResult

    assert(result == 0)
  }

  test("getResult: from a partially complete cell") {
    implicit val pool = new HandlerPool[Immutability]
    val completer = mkCompleter[Immutability]
    val cell = completer.cell

    completer.putNext(ConditionallyImmutable)

    val res = cell.getResult

    assert(res == ConditionallyImmutable)
  }

  test("putNext: Successful, using ImmutabilityLattce") {
    implicit val pool = new HandlerPool[Immutability](ImmutabilityKey)
    val completer = mkCompleter[Immutability]
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

  test("putNext: concurrency test") {
    implicit val pool = new HandlerPool[Immutability](ImmutabilityKey)

    for (i <- 1 to 10000) {
      val completer = mkCompleter[Immutability]

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

  test("when: different depender") {
    val latch1 = new CountDownLatch(2)

    implicit val pool = new HandlerPool[Int]
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]
    val completer3 = mkCompleter[Int]

    completer2.cell.when(completer1.cell)(_ => {
      FinalOutcome(10)
    })
    completer3.cell.when(completer1.cell)(it => {
      if (it.head._2.get.isInstanceOf[FinalOutcome[_]]) FinalOutcome(10)
      else NoOutcome
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
    implicit val pool = new HandlerPool[Immutability](ImmutabilityKey)

    for (i <- 1 to 10000) {
      val completer = mkCompleter[Immutability]

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

  test("if exception-throwing tasks should still run quiescent handlers") {
    implicit val intMaxLattice: Lattice[Int] = new Lattice[Int] {
      override def join(v1: Int, v2: Int): Int = Math.max(v1, v2)
      val bottom = 0
    }
    val key = new DefaultKey[Int]

    implicit val pool = new HandlerPool(key, unhandledExceptionHandler = { t => /* do nothing */ })
    val completer = mkCompleter[Int]

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

  test("when: called at most once with FinalOutcome") {
    implicit val intMaxLattice: Lattice[Int] = new Lattice[Int] {
      override def join(x: Int, y: Int): Int = Math.max(x, y)
      val bottom = 0
    }
    val key = new DefaultKey[Int]

    implicit val pool = new HandlerPool[Int](key)

    for (_ <- 1 to 100) {
      val latch = new CountDownLatch(1)
      val completer1 = mkCompleter[Int]
      val completer2 = mkCompleter[Int]
      var called = 0
      completer2.cell.when(completer1.cell)(it => {
        if (it.head._2.get.isInstanceOf[FinalOutcome[_]]) {
          assert(called === 0)
          called += 1
          latch.countDown()
        }
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

  test("when: discard callbacks on completion") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    val n = 10000

    implicit val pool = new HandlerPool[Int](NaturalNumberKey)
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.trigger()

    cell1.when(cell2)(_ => {
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
      override def resolve(cells: Iterable[Cell[Int]]): Iterable[(Cell[Int], Int)] = {
        Seq((cells.head, 42))
      }

      override def fallback(cells: Iterable[Cell[Int]]): Iterable[(Cell[Int], Int)] = {
        cells.map(cell ⇒ (cell, cell.getResult()))
      }

      override def toString = "ReactivePropertyStoreKey"
    }

    implicit val pool = new HandlerPool(parallelism = 1, key = new ReactivePropertyStoreKey())
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]
    val cell1 = completer1.cell
    val cell2 = completer2.cell

    val completer10 = mkCompleter[Int]
    val completer20 = mkCompleter[Int]
    val cell10 = completer10.cell
    val cell20 = completer20.cell

    completer2.putNext(1)
    cell2.when(cell1)(it => {
      if (it.head._2.get.value == 42) {
        completer2.putFinal(43)
      }
      NoOutcome
    })

    completer20.putNext(1)
    cell20.when(cell10)(it => {
      if (it.head._2.get.value == 10) {
        completer20.putFinal(43)
      }
      NoOutcome
    })

    completer1.putNext(10)
    completer10.putNext(10)

    cell1.when(cell1)(_ => {
      NoOutcome
    })

    cell1.trigger()
    cell2.trigger()
    cell10.trigger()
    cell20.trigger()

    val fut = pool.quiescentResolveCell
    Await.ready(fut, 2.seconds)

    val fut2 = pool.quiescentResolveCell
    Await.ready(fut2, 2.seconds)

  }

  test("recursive quiescentResolveCell using resolve") {
    implicit val pool = new HandlerPool[Int](new RecursiveQuiescentTestKey)
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell2.trigger()

    cell1.when(cell1)(_ => {
      Thread.sleep(200)
      NoOutcome
    })

    cell2.when(cell1)(it => {
      Thread.sleep(200)
      FinalOutcome(it.head._2.get.value * 2)
    })

    val fut = pool.quiescentResolveCell
    Await.ready(fut, 2.seconds)

    assert(completer1.cell.getResult == 42)
    assert(completer2.cell.getResult == 84)

    pool.onQuiescenceShutdown()
  }

  test("recursive quiescentResolveCell using fallback") {
    implicit val pool = new HandlerPool[Int](new RecursiveQuiescentTestKey)
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]
    val cell1 = completer1.cell
    val cell2 = completer2.cell

    cell1.when(cell2)(it => {
      Thread.sleep(200)
      FinalOutcome(it.head._2.get.value * 2)
    })

    val fut = pool.quiescentResolveCell
    Await.ready(fut, 2.seconds)

    assert(completer1.cell.getResult == 86)
    assert(completer2.cell.getResult == 43)

    pool.onQuiescenceShutdown()
  }

  class RecursiveQuiescentTestKey extends Key[Int] {
    override def resolve(cells: Iterable[Cell[Int]]): Iterable[(Cell[Int], Int)] = {
      Seq((cells.head, 42))
    }

    override def fallback(cells: Iterable[Cell[Int]]): Iterable[(Cell[Int], Int)] = {
      cells.map(cell ⇒ (cell, 43))
    }
  }
}

class ConcurrentBaseSuite extends BaseSuite with ConcurrentCompleterFactory

class SequentialBaseSuite extends BaseSuite with SequentialCompleterFactory