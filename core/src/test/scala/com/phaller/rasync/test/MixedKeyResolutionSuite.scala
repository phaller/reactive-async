package com.phaller.rasync.test

import java.util.concurrent.CountDownLatch

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
class MixedKeyResolutionSuite extends FunSuite with MixedCompleterFactory {
  def forwardAsNext[E >: Null](upd: Iterable[(Cell[Int, E], Try[ValueOutcome[Int]])]): Outcome[Int] = {
    val c = upd.head._2
    NextOutcome(c.get.value)
  }

  implicit val intUpdater: Updater[Int] = new IntUpdater

  test("DefaultKey.resolve 1") {
    val k = new DefaultKey[Int, Null]
    implicit val pool = new HandlerPool[Int, Null](k)
    val completer1 = mkSeqCompleter[Int, Null]
    val completer2 = mkConCompleter[Int, Null]
    completer1.cell.when(completer2.cell)(forwardAsNext)
    completer2.cell.when(completer1.cell)(forwardAsNext)
    completer1.putNext(5)
    Await.ready(pool.quiescentResolveCell, 2.seconds)
    assert(completer1.cell.isComplete)
    assert(completer2.cell.isComplete)
    assert(completer1.cell.getResult() == 5)
    assert(completer2.cell.getResult() == 5)
    pool.shutdown()
  }

  test("DefaultKey.resolve 2") {
    val k = new DefaultKey[Int, Null]
    implicit val pool = new HandlerPool[Int, Null](k)
    val completer1 = mkConCompleter[Int, Null]
    val completer2 = mkSeqCompleter[Int, Null]
    completer1.cell.when(completer2.cell)(forwardAsNext)
    completer2.cell.when(completer1.cell)(forwardAsNext)
    completer1.putNext(5)
    Await.ready(pool.quiescentResolveCell, 2.seconds)
    assert(completer1.cell.isComplete)
    assert(completer2.cell.isComplete)
    assert(completer1.cell.getResult() == 5)
    assert(completer2.cell.getResult() == 5)
    pool.shutdown()
  }

  test("when: cSCC with constant resolution 1") {
    val latch = new CountDownLatch(4)

    object ConstantKey extends Key[Int, Null] {
      val RESOLVEDINCYCLE = 5
      val RESOLVEDASINDPENDENT = 10

      override def resolve(cells: Iterable[Cell[Int, Null]]): Iterable[(Cell[Int, Null], Int)] = cells.map((_, RESOLVEDINCYCLE))

      override def fallback(cells: Iterable[Cell[Int, Null]]): Iterable[(Cell[Int, Null], Int)] = cells.map((_, RESOLVEDASINDPENDENT))
    }

    implicit val pool = new HandlerPool[Int, Null](ConstantKey)

    val completer1 = mkConCompleter[Int, Null]
    val cell1 = completer1.cell
    val completer2 = mkConCompleter[Int, Null]
    val cell2 = completer2.cell
    val completer3 = mkSeqCompleter[Int, Null]
    val cell3 = completer3.cell
    val completer4 = mkSeqCompleter[Int, Null]
    val cell4 = completer4.cell

    // set unwanted values:
    completer1.putNext(-1)
    completer2.putNext(-1)
    completer3.putNext(-1)
    completer4.putNext(-1)

    // create a cSCC, assert that none of the callbacks get called again.
    def c(upd: Iterable[(Cell[Int, Null], Try[ValueOutcome[Int]])]): Outcome[Int] = upd.head._2.get match {
      case FinalOutcome(_) =>
        NoOutcome
      case NextOutcome(-1) =>
        NoOutcome
      case _ =>
        assert(false)
        NextOutcome(-2)
    }

    cell1.when(cell2)(c)
    cell1.when(cell3)(c)
    cell2.when(cell4)(c)
    cell3.when(cell4)(c)
    cell4.when(cell1)(c)

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

  test("when: cSCC with constant resolution 2") {
    val latch = new CountDownLatch(4)

    object ConstantKey extends Key[Int, Null] {
      val RESOLVEDINCYCLE = 5
      val RESOLVEDASINDPENDENT = 10

      override def resolve(cells: Iterable[Cell[Int, Null]]): Iterable[(Cell[Int, Null], Int)] = cells.map((_, RESOLVEDINCYCLE))

      override def fallback(cells: Iterable[Cell[Int, Null]]): Iterable[(Cell[Int, Null], Int)] = cells.map((_, RESOLVEDASINDPENDENT))
    }

    implicit val pool = new HandlerPool[Int, Null](ConstantKey)

    val completer1 = mkConCompleter[Int, Null]
    val cell1 = completer1.cell
    val completer2 = mkSeqCompleter[Int, Null]
    val cell2 = completer2.cell
    val completer3 = mkSeqCompleter[Int, Null]
    val cell3 = completer3.cell
    val completer4 = mkConCompleter[Int, Null]
    val cell4 = completer4.cell

    // set unwanted values:
    completer1.putNext(-1)
    completer2.putNext(-1)
    completer3.putNext(-1)
    completer4.putNext(-1)

    // create a cSCC, assert that none of the callbacks get called again.
    def c(upd: Iterable[(Cell[Int, Null], Try[ValueOutcome[Int]])]): Outcome[Int] = upd.head._2.get match {
      case FinalOutcome(_) =>
        NoOutcome
      case NextOutcome(-1) =>
        NoOutcome
      case _ =>
        assert(false)
        NextOutcome(-2)
    }

    cell1.when(cell2)(c)
    cell1.when(cell3)(c)
    cell2.when(cell4)(c)
    cell3.when(cell4)(c)
    cell4.when(cell1)(c)

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

  test("when: cSCC with default resolution 1") {
    val latch = new CountDownLatch(4)

    implicit val pool = new HandlerPool[Int, Null]

    val completer1 = mkSeqCompleter[Int, Null]
    val cell1 = completer1.cell
    val completer2 = mkConCompleter[Int, Null]
    val cell2 = completer2.cell
    val completer3 = mkSeqCompleter[Int, Null]
    val cell3 = completer3.cell
    val completer4 = mkConCompleter[Int, Null]
    val cell4 = completer4.cell

    // set unwanted values:
    completer1.putNext(-1)
    completer2.putNext(-1)
    completer3.putNext(-1)
    completer4.putNext(-1)

    // create a cSCC, assert that none of the callbacks get called again.
    def c(upd: Iterable[(Cell[Int, Null], Try[ValueOutcome[Int]])]): Outcome[Int] = upd.head._2.get match {
      case FinalOutcome(_) =>
        NoOutcome
      case NextOutcome(-1) =>
        NoOutcome
      case _ =>
        assert(false)
        NextOutcome(-2)
    }

    cell1.when(cell2)(c)
    cell1.when(cell3)(c)
    cell2.when(cell4)(c)
    cell3.when(cell4)(c)
    cell4.when(cell1)(c)

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

  test("when: cSCC with default resolution 2") {
    val latch = new CountDownLatch(4)

    implicit val pool = new HandlerPool[Int, Null]

    val completer1 = mkConCompleter[Int, Null]
    val cell1 = completer1.cell
    val completer2 = mkConCompleter[Int, Null]
    val cell2 = completer2.cell
    val completer3 = mkSeqCompleter[Int, Null]
    val cell3 = completer3.cell
    val completer4 = mkConCompleter[Int, Null]
    val cell4 = completer4.cell

    // set unwanted values:
    completer1.putNext(-1)
    completer2.putNext(-1)
    completer3.putNext(-1)
    completer4.putNext(-1)

    // create a cSCC, assert that none of the callbacks get called again.
    def c(upd: Iterable[(Cell[Int, Null], Try[ValueOutcome[Int]])]): Outcome[Int] = upd.head._2.get match {
      case FinalOutcome(_) =>
        NoOutcome
      case NextOutcome(-1) =>
        NoOutcome
      case _ =>
        assert(false)
        NextOutcome(-2)
    }

    cell1.when(cell2)(c)
    cell1.when(cell3)(c)
    cell2.when(cell4)(c)
    cell3.when(cell4)(c)
    cell4.when(cell1)(c)

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

  test("when: cycle with default resolution 1") {
    sealed trait Value
    case object Bottom extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val bottom: Value = Bottom
    }

    implicit val pool: HandlerPool[Value, Null] = new HandlerPool[Value, Null]

    for (i <- 1 to 100) {
      val completer1 = mkConCompleter[Value, Null]
      val completer2 = mkSeqCompleter[Value, Null]
      val cell1 = completer1.cell
      val cell2 = completer2.cell

      cell1.when(cell2)(_ => NextOutcome(ShouldNotHappen))
      cell2.when(cell1)(_ => NextOutcome(ShouldNotHappen))

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 1.minutes)

      assert(cell1.getResult() != ShouldNotHappen)
      assert(cell2.getResult() != ShouldNotHappen)
    }

    pool.onQuiescenceShutdown()
  }

  test("when: cycle with default resolution 2") {
    sealed trait Value
    case object Bottom extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val bottom: Value = Bottom
    }

    implicit val pool: HandlerPool[Value, Null] = new HandlerPool[Value, Null]

    for (i <- 1 to 100) {
      val completer1 = mkSeqCompleter[Value, Null]
      val completer2 = mkConCompleter[Value, Null]
      val cell1 = completer1.cell
      val cell2 = completer2.cell

      cell1.when(cell2)(_ => NextOutcome(ShouldNotHappen))
      cell2.when(cell1)(_ => NextOutcome(ShouldNotHappen))

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 1.minutes)

      assert(cell1.getResult() != ShouldNotHappen)
      assert(cell2.getResult() != ShouldNotHappen)
    }

    pool.onQuiescenceShutdown()
  }

  test("when: cycle with constant resolution 1") {
    sealed trait Value
    case object Bottom extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = if (v1 == Bottom) v2 else v1 // TODO or throw?
      override val bottom: Value = Bottom
    }

    object TheKey extends DefaultKey[Value, Null] {
      override def resolve(cells: Iterable[Cell[Value, Null]]): Iterable[(Cell[Value, Null], Value)] = {
        cells.map(cell => (cell, OK))
      }
    }

    implicit val pool = new HandlerPool[Value, Null](TheKey)

    for (i <- 1 to 100) {
      val completer1 = mkConCompleter[Value, Null]
      val completer2 = mkSeqCompleter[Value, Null]
      val cell1 = completer1.cell
      val cell2 = completer2.cell

      cell1.when(cell2)(_ => NextOutcome(ShouldNotHappen))
      cell2.when(cell1)(_ => NextOutcome(ShouldNotHappen))

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 1.minutes)

      assert(cell1.getResult() == OK)
      assert(cell2.getResult() == OK)
    }

    pool.onQuiescenceShutdown()
  }

  test("when: cycle with constant resolution 2") {
    sealed trait Value
    case object Bottom extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = if (v1 == Bottom) v2 else v1 // TODO or throw?
      override val bottom: Value = Bottom
    }

    object TheKey extends DefaultKey[Value, Null] {
      override def resolve(cells: Iterable[Cell[Value, Null]]): Iterable[(Cell[Value, Null], Value)] = {
        cells.map(cell => (cell, OK))
      }
    }

    implicit val pool = new HandlerPool[Value, Null](TheKey)

    for (i <- 1 to 100) {
      val completer1 = mkSeqCompleter[Value, Null]
      val completer2 = mkConCompleter[Value, Null]
      val cell1 = completer1.cell
      val cell2 = completer2.cell

      cell1.when(cell2)(_ => NextOutcome(ShouldNotHappen))
      cell2.when(cell1)(_ => NextOutcome(ShouldNotHappen))

      val fut = pool.quiescentResolveCell
      Await.ready(fut, 1.minutes)

      assert(cell1.getResult() == OK)
      assert(cell2.getResult() == OK)
    }

    pool.onQuiescenceShutdown()
  }

  test("whenNext: cycle with additional outgoing dep 1") {
    sealed trait Value
    case object Bottom extends Value
    case object Resolved extends Value
    case object Fallback extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val bottom: Value = Bottom
    }

    object TheKey extends DefaultKey[Value, Null] {
      override def resolve(cells: Iterable[Cell[Value, Null]]): Iterable[(Cell[Value, Null], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback(cells: Iterable[Cell[Value, Null]]): Iterable[(Cell[Value, Null], Value)] = {
        cells.map(cell => (cell, Fallback))
      }
    }

    implicit val pool = new HandlerPool[Value, Null](TheKey)
    val completer1 = mkConCompleter[Value, Null]
    val completer2 = mkConCompleter[Value, Null]
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val out = mkSeqCompleter[Value, Null]

    // let `cell1` and `cell2` form a cycle
    cell1.when(cell2)(_ => NextOutcome(ShouldNotHappen))
    cell2.when(cell1)(_ => NextOutcome(ShouldNotHappen))

    // the cycle is dependent on incoming information from `out`
    cell2.when(out.cell)(_ => NextOutcome(ShouldNotHappen))

    // resolve the independent cell `out` and the cycle
    val fut = pool.quiescentResolveCell
    Await.ready(fut, 1.minutes)

    pool.onQuiescenceShutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(out.cell.getResult() == Fallback)
  }

  test("whenNext: cycle with additional outgoing dep 2") {
    sealed trait Value
    case object Bottom extends Value
    case object Resolved extends Value
    case object Fallback extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val bottom: Value = Bottom
    }

    object TheKey extends DefaultKey[Value, Null] {
      override def resolve(cells: Iterable[Cell[Value, Null]]): Iterable[(Cell[Value, Null], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback(cells: Iterable[Cell[Value, Null]]): Iterable[(Cell[Value, Null], Value)] = {
        cells.map(cell => (cell, Fallback))
      }
    }

    implicit val pool = new HandlerPool[Value, Null](TheKey)
    val completer1 = mkSeqCompleter[Value, Null]
    val completer2 = mkConCompleter[Value, Null]
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val out = mkSeqCompleter[Value, Null]

    // let `cell1` and `cell2` form a cycle
    cell1.when(cell2)(_ => NextOutcome(ShouldNotHappen))
    cell2.when(cell1)(_ => NextOutcome(ShouldNotHappen))

    // the cycle is dependent on incoming information from `out`
    cell2.when(out.cell)(_ => NextOutcome(ShouldNotHappen))

    // resolve the independent cell `out` and the cycle
    val fut = pool.quiescentResolveCell
    Await.ready(fut, 1.minutes)

    pool.onQuiescenceShutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(out.cell.getResult() == Fallback)
  }

  test("whenNext: cycle with additional incoming dep 1") {
    sealed trait Value
    case object Bottom extends Value
    case object Dummy extends Value
    case object Resolved extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val bottom: Value = Bottom
    }

    object TheKey extends DefaultKey[Value, Null] {
      override def resolve(cells: Iterable[Cell[Value, Null]]): Iterable[(Cell[Value, Null], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback(cells: Iterable[Cell[Value, Null]]): Iterable[(Cell[Value, Null], Value)] = {
        Seq()
      }
    }

    implicit val pool = new HandlerPool[Value, Null](TheKey)
    val completer1 = mkSeqCompleter[Value, Null]
    val completer2 = mkSeqCompleter[Value, Null]
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val in = mkConCompleter[Value, Null]
    in.putNext(Dummy)
    cell1.when(cell2)(_ => NextOutcome(ShouldNotHappen))
    cell2.when(cell1)(_ => NextOutcome(ShouldNotHappen))
    in.putNext(ShouldNotHappen)
    in.cell.when(cell1)(_ => FinalOutcome(OK))

    val fut = pool.quiescentResolveCell
    Await.ready(fut, 1.minutes)

    pool.onQuiescenceShutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(in.cell.getResult() == OK)
  }

  test("whenNext: cycle with additional incoming dep 2") {
    sealed trait Value
    case object Bottom extends Value
    case object Dummy extends Value
    case object Resolved extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val bottom: Value = Bottom
    }

    object TheKey extends Key[Value, Null] {
      override def resolve(cells: Iterable[Cell[Value, Null]]): Iterable[(Cell[Value, Null], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback(cells: Iterable[Cell[Value, Null]]): Iterable[(Cell[Value, Null], Value)] = {
        Seq()
      }
    }

    implicit val pool = new HandlerPool[Value, Null](TheKey)
    val completer1 = mkConCompleter[Value, Null]
    val completer2 = mkConCompleter[Value, Null]
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val in = mkSeqCompleter[Value, Null]
    in.putNext(Dummy)
    cell1.when(cell2)(_ => NextOutcome(ShouldNotHappen))
    cell2.when(cell1)(_ => NextOutcome(ShouldNotHappen))
    in.putNext(ShouldNotHappen)
    in.cell.when(cell1)(_ => FinalOutcome(OK))

    val fut = pool.quiescentResolveCell
    Await.ready(fut, 1.minutes)

    pool.onQuiescenceShutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(in.cell.getResult() == OK)
  }

}
