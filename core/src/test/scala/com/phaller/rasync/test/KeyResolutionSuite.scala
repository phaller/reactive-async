package com.phaller.rasync.test

import java.util.concurrent.CountDownLatch

import com.phaller.rasync._
import com.phaller.rasync.cell._
import com.phaller.rasync.lattice.{ DefaultKey, Key, Updater }
import com.phaller.rasync.pool.HandlerPool
import com.phaller.rasync.test.lattice.IntUpdater
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

/**
 * Tests where cylces or independent cells
 * need to be resolved via a Key.
 * This tests contains cycles that only constist
 * of a single type of Cells and do not mix
 * SequentialCells and ConcurrentCells.
 * For the mixedcase, see MixedKeyResolutionsuite
 */
abstract class KeyResolutionSuite extends FunSuite with CompleterFactory {
  implicit val intUpdater: Updater[Int] = new IntUpdater

  test("DefaultKey.resolve") {
    val k = new DefaultKey[Int]
    implicit val pool = new HandlerPool[Int](k)
    val completer1 = mkCompleter[Int]
    val completer2 = mkCompleter[Int]
    completer1.cell.when((_, x) => NextOutcome(x.get.value), completer2.cell)
    completer2.cell.when((_, x) => NextOutcome(x.get.value), completer1.cell)
    completer1.putNext(5)
    Await.ready(pool.quiescentResolveCell, 2.seconds)
    assert(completer1.cell.isComplete)
    assert(completer2.cell.isComplete)
    assert(completer1.cell.getResult() == 5)
    assert(completer2.cell.getResult() == 5)
    pool.shutdown()
  }

  test("DefaultKey.fallback") {
    val k = new DefaultKey[Int]
    implicit val pool = new HandlerPool[Int](k)
    val completer1 = mkCompleter[Int]
    completer1.cell.trigger()
    completer1.putNext(5)
    Await.ready(pool.quiescentResolveCell, 2.seconds)
    assert(completer1.cell.isComplete)
    assert(completer1.cell.getResult() == 5)
    pool.shutdown()
  }

  test("when: cSCC with constant resolution") {
    val latch = new CountDownLatch(4)

    object ConstantKey extends Key[Int] {
      val RESOLVEDINCYCLE = 5
      val RESOLVEDASINDPENDENT = 10

      override def resolve(cells: Iterable[Cell[Int]]): Iterable[(Cell[Int], Int)] = cells.map((_, RESOLVEDINCYCLE))

      override def fallback(cells: Iterable[Cell[Int]]): Iterable[(Cell[Int], Int)] = cells.map((_, RESOLVEDASINDPENDENT))
    }

    implicit val pool = new HandlerPool[Int](ConstantKey)

    val completer1 = mkCompleter[Int]
    val cell1 = completer1.cell
    val completer2 = mkCompleter[Int]
    val cell2 = completer2.cell
    val completer3 = mkCompleter[Int]
    val cell3 = completer3.cell
    val completer4 = mkCompleter[Int]
    val cell4 = completer4.cell

    // set unwanted values:
    completer1.putNext(-1)
    completer2.putNext(-1)
    completer3.putNext(-1)
    completer4.putNext(-1)

    // create a cSCC, assert that none of the callbacks get called again.
    def c(cell: Cell[Int], v: Try[ValueOutcome[Int]]): Outcome[Int] = v.get match {
      case FinalOutcome(_) =>
        NoOutcome
      case NextOutcome(-1) =>
        NoOutcome
      case _ =>
        assert(false)
        NextOutcome(-2)
    }

    cell1.when(c, cell2)
    cell1.when(c, cell3)
    cell2.when(c, cell4)
    cell3.when(c, cell4)
    cell4.when(c, cell1)

    for (c <- List(cell1, cell2, cell3, cell4))
      c.onComplete {
        case Success(v) =>
          assert(v === ConstantKey.RESOLVEDINCYCLE)
          assert(c.numDependencies === 0)
          latch.countDown()
        case Failure(e) =>
          assert(false)
          latch.countDown()
      }

    // resolve cells
    val fut = pool.quiescentResolveCell
    Await.result(fut, 2.seconds)
    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("when: cSCC with default resolution") {
    val latch = new CountDownLatch(4)

    implicit val pool = new HandlerPool[Int]

    val completer1 = mkCompleter[Int]
    val cell1 = completer1.cell
    val completer2 = mkCompleter[Int]
    val cell2 = completer2.cell
    val completer3 = mkCompleter[Int]
    val cell3 = completer3.cell
    val completer4 = mkCompleter[Int]
    val cell4 = completer4.cell

    // set unwanted values:
    completer1.putNext(-1)
    completer2.putNext(-1)
    completer3.putNext(-1)
    completer4.putNext(-1)

    // create a cSCC, assert that none of the callbacks get called again.
    def c(cell: Cell[Int], v: Try[ValueOutcome[Int]]): Outcome[Int] = v.get match {
      case FinalOutcome(_) =>
        NoOutcome
      case NextOutcome(-1) =>
        NoOutcome
      case _ =>
        assert(false)
        NextOutcome(-2)
    }

    cell1.when(c, cell2)
    cell1.when(c, cell3)
    cell2.when(c, cell4)
    cell3.when(c, cell4)
    cell4.when(c, cell1)

    for (c <- List(cell1, cell2, cell3, cell4))
      c.onComplete {
        case Success(v) =>
          assert(v === -1)
          assert(c.numDependencies === 0)
          latch.countDown()
        case Failure(e) =>
          assert(false)
          latch.countDown()
      }

    // resolve cells
    val fut = pool.quiescentResolveCell
    Await.result(fut, 2.seconds)
    latch.await()

    pool.onQuiescenceShutdown()
  }

  test("when: cycle with default resolution") {
    sealed trait Value
    case object Bottom extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val initial: Value = Bottom
    }

    implicit val pool: HandlerPool[Value] = new HandlerPool[Value]

    for (i <- 1 to 100) {
      val completer1 = mkCompleter[Value]
      val completer2 = mkCompleter[Value]
      val cell1 = completer1.cell
      val cell2 = completer2.cell

      cell1.when((_, _) => NextOutcome(ShouldNotHappen), cell2)
      cell2.when((_, _) => NextOutcome(ShouldNotHappen), cell1)

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 1.minutes)

      assert(cell1.getResult() != ShouldNotHappen)
      assert(cell2.getResult() != ShouldNotHappen)
    }

    pool.onQuiescenceShutdown()
  }

  test("when: cycle with constant resolution") {
    sealed trait Value
    case object Bottom extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = if (v1 == Bottom) v2 else v1 // TODO or throw?
      override val initial: Value = Bottom
    }

    object TheKey extends DefaultKey[Value] {
      override def resolve(cells: Iterable[Cell[Value]]): Iterable[(Cell[Value], Value)] = {
        cells.map(cell => (cell, OK))
      }
    }

    implicit val pool = new HandlerPool[Value](TheKey)

    for (i <- 1 to 100) {
      val completer1 = mkCompleter[Value]
      val completer2 = mkCompleter[Value]
      val cell1 = completer1.cell
      val cell2 = completer2.cell

      cell1.when((_, v) => NextOutcome(ShouldNotHappen), cell2)
      cell2.when((_, v) => NextOutcome(ShouldNotHappen), cell1)

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 1.minutes)

      assert(cell1.getResult() == OK)
      assert(cell2.getResult() == OK)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNext: cycle with additional outgoing dep") {
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
      override def resolve(cells: Iterable[Cell[Value]]): Iterable[(Cell[Value], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback(cells: Iterable[Cell[Value]]): Iterable[(Cell[Value], Value)] = {
        cells.map(cell => (cell, Fallback))
      }
    }

    implicit val pool = new HandlerPool[Value](TheKey)
    val completer1 = mkCompleter[Value]
    val completer2 = mkCompleter[Value]
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val out = mkCompleter[Value]

    // let `cell1` and `cell2` form a cycle
    cell1.when((_, _) => NextOutcome(ShouldNotHappen), cell2)
    cell2.when((_, _) => NextOutcome(ShouldNotHappen), cell1)

    // the cycle is dependent on incoming information from `out`
    cell2.when((_, _) => NextOutcome(ShouldNotHappen), out.cell)

    // resolve the independent cell `out` and the cycle
    val fut = pool.quiescentResolveCell
    Await.ready(fut, 1.minutes)

    pool.onQuiescenceShutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(out.cell.getResult() == Fallback)
  }

  test("whenNext: cycle with additional incoming dep") {
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
      override def resolve(cells: Iterable[Cell[Value]]): Iterable[(Cell[Value], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback(cells: Iterable[Cell[Value]]): Iterable[(Cell[Value], Value)] = {
        Seq()
      }
    }

    implicit val pool = new HandlerPool[Value](TheKey)
    val completer1 = mkCompleter[Value]
    val completer2 = mkCompleter[Value]
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val in = mkCompleter[Value]
    in.putNext(Dummy)
    cell1.when((_, _) => NextOutcome(ShouldNotHappen), cell2)
    cell2.when((_, _) => NextOutcome(ShouldNotHappen), cell1)
    in.putNext(ShouldNotHappen)
    in.cell.when((_, _) => FinalOutcome(OK), cell1)

    val fut = pool.quiescentResolveCell
    Await.ready(fut, 1.minutes)

    pool.onQuiescenceShutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(in.cell.getResult() == OK)
  }
}

class ConcurrentKeyResolutionSuite extends KeyResolutionSuite with ConcurrentCompleterFactory

class SequentialKeyResolutionSuite extends KeyResolutionSuite with SequentialCompleterFactory
