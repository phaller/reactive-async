import org.scalatest.FunSuite

import java.util.concurrent.CountDownLatch

import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global

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

    assert(true)
  }
}
