package com.phaller.rasync
package test

import scala.language.implicitConversions
import java.util.concurrent.{ CountDownLatch, TimeUnit }

import scala.concurrent.duration._
import com.phaller.rasync.lattice._
import com.phaller.rasync.pool.HandlerPool
import com.phaller.rasync.cell._
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.util.{ Failure, Success }

class ExceptionSuite extends FunSuite {
  /*
   * This Suite contains test cases, where exceptions are thrown
   * - when initializing cells,
   * - in dependency callbacks
   * - in a Key's methods (fallback, resolve)
   *
   * It also tests, whether completed cells (with Success or Failure)
   * behave correclty wrt. exceptions in dependencies and keys.
   */

  implicit val naturalNumberUpdater: Updater[Int] = Updater.latticeToUpdater(new NaturalNumberLattice)
  implicit def strToIntKey(s: String): NaturalNumberKey.type = NaturalNumberKey

  test("exception in init") {
    // If the init method throws an exception e,
    // the cell is completed with Failure(e)
    val latch = new CountDownLatch(1)
    val pool = new HandlerPool[Int](NaturalNumberKey)
    val cell = pool.mkCell(_ => {
      throw new Exception("foo")
    })

    // cell should be completed as a consequence
    cell.onComplete(_ => latch.countDown())

    assert(!cell.isComplete)
    cell.trigger()

    // wait for cell to be completed
    latch.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    cell.getTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "foo")
    }

    pool.shutdown()
  }

  test("exception in concurrent callback") {
    // If the callback thrown an exception e,
    // the dependent cell is completed with Failure e
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool[Int] = new HandlerPool[Int](NaturalNumberKey)
    val c0 = CellCompleter()
    val cell = pool.mkCell(c => {
      // build up dependency, throw exeption, if c0's value changes
      c.when(c0.cell)(_ => throw new Exception("foo"))
      NoOutcome
    })

    // cell should be completed as a consequence
    cell.onComplete(_ => latch.countDown())
    cell.trigger()

    assert(!cell.isComplete)

    // trigger dependency, s.t. callback is called
    c0.putFinal(1)

    // wait for cell to be completed
    latch.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    cell.getTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "foo")
    }

    pool.shutdown()
  }

  test("exception in sequential callback") {
    // If the callback thrown an exception e,
    // the dependent cell is completed with Failure e
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool[Int] = new HandlerPool(NaturalNumberKey)
    val c0 = CellCompleter()
    val cell = pool.mkSequentialCell(c => {
      // build up dependency, throw exeption, if c0's value changes
      c.when(c0.cell)(_ => throw new Exception("foo"))
      NoOutcome
    })

    // cell should be completed as a consequence
    cell.onComplete(_ => latch.countDown())
    cell.trigger()

    assert(!cell.isComplete)

    // trigger dependency, s.t. callback is called
    c0.putFinal(1)

    // wait for cell to be completed
    latch.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    cell.getTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "foo")
    }

    pool.shutdown()
  }

  test("exception in Key.resolve") {
    // If Key.resolved is called for cSSC of cells c
    // and throws an exception e, all cells c are completed
    // with Failure(e).

    // Define a key that throws exceptions
    object ExceptionKey extends Key[Int] {
      override def resolve(cells: Iterable[Cell[Int]]): Iterable[(Cell[Int], Int)] =
        throw new Exception("foo")

      override def fallback(cells: Iterable[Cell[Int]]): Iterable[(Cell[Int], Int)] =
        throw new Exception("bar")
    }

    implicit val pool: HandlerPool[Int] = new HandlerPool(ExceptionKey)
    val c0 = CellCompleter()
    val c1 = CellCompleter()
    val c2 = CellCompleter()
    val c3 = CellCompleter()
    val c4 = CellCompleter()

    // Create a cSSC
    c1.cell.when(c0.cell)(_ => NoOutcome)
    c2.cell.when(c1.cell)(_ => NoOutcome)
    c3.cell.when(c1.cell)(_ => NoOutcome)
    c4.cell.when(c2.cell)(_ => NoOutcome)
    c4.cell.when(c3.cell)(_ => NoOutcome)
    c0.cell.when(c4.cell)(_ => NoOutcome)

    // wait for the cycle to be resolved by ExceptionKey.resolve
    Await.ready(pool.quiescentResolveCell, 2.seconds)

    pool.onQuiescenceShutdown()

    // check for exceptions in all cells of the cycle
    for (c ← List(c0, c1, c2, c3, c4))
      c.cell.getTry() match {
        case Success(_) => assert(false)
        case Failure(e) => assert(e.getMessage == "foo")
      }
  }

  test("exception in Key.fallback") {
    // If Key.fallback is called for a cell c
    // and throws an exception e, c is completed
    // with Failure(e).
    // A depedent cell receives the failure.

    // Define a key that throws exceptions
    object ExceptionKey extends Key[Int] {
      override def resolve(cells: Iterable[Cell[Int]]): Iterable[(Cell[Int], Int)] =
        throw new Exception("foo")

      override def fallback(cells: Iterable[Cell[Int]]): Iterable[(Cell[Int], Int)] =
        throw new Exception("bar")
    }

    implicit val pool: HandlerPool[Int] = new HandlerPool(ExceptionKey)
    val triggerLatch = new CountDownLatch(2)
    val c0 = CellCompleter[Int](_ => { triggerLatch.countDown(); NoOutcome })
    val c1 = CellCompleter[Int](_ => { triggerLatch.countDown(); NoOutcome })

    c1.cell.trigger()
    c0.cell.trigger()
    triggerLatch.await()

    // Create a dependency, c1 "recover" from the exception in c0 by completing with 10
    c1.cell.when(c0.cell)(_ => FinalOutcome(10))

    // wait for c0 to be resolved by ExceptionKey.fallback
    Await.ready(pool.quiescentResolveCell, 2.seconds)

    // c0 should have been resolved with Failure("bar")
    c0.cell.getTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "bar")
    }
    // c1 should have ignored this failure and contain 10
    assert(c1.cell.isComplete)
    assert(c1.cell.getResult() == 10)

    pool.shutdown()
  }

  test("exception after freeze") {
    // after a cell has been completed, an exception in one
    // of its callbacks should be ignored
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool[Int] = new HandlerPool(NaturalNumberKey)
    val c0 = CellCompleter()
    val c1 = CellCompleter()
    val c2 = CellCompleter()

    // Create a dependency, c1 "recover" from the exception in c0 by completing with 10
    c2.cell.when(c0.cell)(_ => FinalOutcome(10))
    c2.cell.when(c1.cell)(_ => throw new Exception("BOOM"))
    c2.cell.onComplete(_ => latch.countDown())

    // trigger completion of cell2
    c0.putFinal(0)
    // wait form completion of cell2
    latch.await(2, TimeUnit.SECONDS)
    // trigger an exception-throwing callback (this should be ignored)
    c1.putFinal(1)

    pool.onQuiescent(() => {
      // c2 should have been completed after c0.putFinal(…),
      // so the exception should not matter
      assert(c2.cell.isComplete)
      assert(c2.cell.getResult() == 10)

      pool.shutdown()
    })
  }

  test("put after completion with exception") {
    // after a cell has been completed with an exception,
    // any subsequent put should be ignored.
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool[Int] = new HandlerPool(NaturalNumberKey)
    val c0 = CellCompleter()
    val c1 = CellCompleter()
    val c2 = CellCompleter()

    // Create dependencies
    c2.cell.when(c0.cell)(_ => FinalOutcome(10))
    c2.cell.when(c1.cell)(_ => throw new Exception("foo"))
    c2.cell.onComplete(_ => latch.countDown())

    c1.putFinal(1)
    latch.await(2, TimeUnit.SECONDS)
    c0.putFinal(0)

    pool.onQuiescent(() => {

      // c2 should have been completed after c1.putFinal(…),
      // so the FinalOutcome(10) should be ignored
      assert(c2.cell.isComplete)
      c2.cell.getTry() match {
        case Success(_) => assert(false)
        case Failure(e) => assert(e.getMessage == "foo")
      }

      pool.shutdown()
    })

  }

  test("do not catch fatal exception") {
    // If an instance of Error is thrown,
    // this will not be used as value for
    // the respective cell.
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)
    implicit val pool: HandlerPool[Int] = new HandlerPool(NaturalNumberKey, unhandledExceptionHandler = _ => latch1.countDown())
    val c0 = CellCompleter()
    val cell = pool.mkCell(c => {
      // build up dependency, throw error, if c0's value changes
      c.when(c0.cell)(_ => throw new Error("It's OK, if I am not caught. See description"))
      NoOutcome
    })

    cell.trigger()

    // trigger dependency, s.t. callback is called
    // this causes the error to be thrown.
    // This should not be handled internally.
    c0.putFinal(1)
    // error should be caught by exception handler
    latch1.await()

    assert(!cell.isComplete)
    cell.completer.putFinal(10)
    // cell should be completed
    cell.onComplete(_ => latch2.countDown())

    // wait for cell to be completed
    latch2.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    assert(cell.getResult() == 10)

    pool.shutdown()
  }

}
