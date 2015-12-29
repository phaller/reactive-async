import org.scalatest.FunSuite

import java.util.concurrent.CountDownLatch

import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global

import cell.{CellCompleter, HandlerPool}


class BaseSuite extends FunSuite {
  test("putFinal") {
    val latch = new CountDownLatch(1)

    val completer = CellCompleter[String, Int]("somekey")
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
  }

  test("whenComplete") {
    val latch = new CountDownLatch(1)

    val completer1 = CellCompleter[String, Int]("somekey")
    val completer2 = CellCompleter[String, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenComplete(completer2.cell, (x: Int) => x == 10, 20)

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
  }

  test("whenComplete: dependency 1") {
    val latch = new CountDownLatch(1)

    val completer1 = CellCompleter[String, Int]("somekey")
    val completer2 = CellCompleter[String, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenComplete(completer2.cell, (x: Int) => x == 10, 20)

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

    assert(cell1.dependencies.isEmpty)
  }

  test("whenComplete: dependency 2") {
    val completer1 = CellCompleter[String, Int]("somekey")
    val completer2 = CellCompleter[String, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenComplete(completer2.cell, (x: Int) => x == 10, 20)

    completer2.putFinal(9)

    cell1.waitUntilNoDeps()

    assert(cell1.dependencies.isEmpty)
  }

  test("handler pool") {
    val pool = new HandlerPool
    val latch = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)
    pool.execute { () => latch.await() }
    pool.onQuiescent { () => latch2.countDown() }
    latch.countDown()

    latch2.await()
    assert(true)
  }
}
